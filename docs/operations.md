# Operations

## OpenCode chat agents

`model.parent topic=<id> provider=opencode` picks OpenCode for one linked chat.
`topic.create provider=opencode` creates a chat with that pick. Both require
`delegation.opencode` to be on and an active account selected with `opencode.use`.
The home chat cannot use OpenCode.

The chat agent runs on the account that `opencode.use` selects. That account
supplies its model. OpenCode chat agents have no thinking setting. A change
of account starts a new native session with a continuity note.

The agent has only Torii tools. It has no built-in shell, file or web tools,
and no other MCP servers. It gives file paths to workers that can read them.
The model host sees the chat's messages, the coordinator prompt and every
Torii tool result that the agent reads. These results can include lists from
other chats.

At usage limits or host errors, the chat pauses on the selected account.
Torii saves new messages and does not fall back to another agent.
API spend is an estimate for the native session. Subscription meters stay in
`/accounts`. A subscription chat shows no spend. Its catalog cost does not
represent the subscription charge.


## Coordinator intake batching

Coordinator intake batching is off by default. Enable it with
`python3 -m coordinator ctl call service.intake_batching enabled=true`.
Use `enabled=false` to turn it off. `settings.get` reports
`coordinator_intake_batching`.

With the switch on, each feed pass admits a batch even while the parent is busy,
running background work or recovering uncertain input. Among pending items,
owner messages, forwarded asks and filled-secret notices come first, then
failures and blockers, then routine input. Each group is oldest first.
Structured failures and event headers such as `Failed:` or `Job 12 blocked:`
receive failure priority.

A batch contains at most 20 whole items and normally at most 8,000 characters,
including item tags and a usage policy refresh. A single larger item travels
alone without truncation. The rest waits for the following turn. Each item keeps
its own message ID, sender, job, creation time and text. Separate owner asks keep
separate reply targets. Audio preparation remains a barrier within its chat.

Batch membership is durable. Native receipts, turn tracking and recovery apply
to every member. Turning the switch off restores individual admission for new
items. An existing batch still retries as one whole input. A previously formed
batch also remains whole if a later policy refresh increases its size.
If recovery changes only some members, the pending remainder loses its old
batch membership and is admitted again. Received or uncertain members are not
replayed. A failed attachment affects only its row; other members are delivered.

Before enabling the switch, run one authorized live Codex check of consecutive
batched turns. Confirm the owner receives one answer and a prior automatic
answer does not suppress the next turn's reply. Offline tests cover this cursor
logic but do not establish live model behavior.


## Worker temp and job results folders

Each job has a retained results folder at `<state>/results/task-<id>`. Torii
creates it with mode 700 on the first `worktree.create` or `workers.spawn`.
`tasks.get`, `worktree.create`, and `workers.spawn` return its path as `results`.
Claude and Codex worker instructions include that path. Put reports, logs,
and proofs there. OpenCode's external-directory permission denies the results
folder, so its instructions omit the path. The parent still receives it.
Torii never deletes or moves results folders.

Worker temp cleanup is always on. Claude and Codex worker processes receive
`TMPDIR=<service temp>/torii-w<worker id>`, with mode 700. The service temp folder
comes from `tempfile.gettempdir()`. The short name leaves room for Unix sockets
on macOS. OpenCode keeps its private `PRIVATE_FOLDERS` temp path. Coordinators'
own sessions are unchanged. Account rotations and quota waits keep temp files.
A worker resumed after a final status gets fresh temp storage. A service
restart reattaches to the running worker's saved temp folder.

Torii removes a worker's temp folder when its status is final and the host proves
its processes are gone. Owned hosts require `process_ownership` cleanup `ok`.
Other hosts accept an exit record; without it, the host and child PIDs must
be dead. An unrelated process reusing an old PID does not block a finished
worker's cleanup or resume. A failed or missing proof retains the folder.
There is no retention timer. Startup and each existing worktree cleanup pass
reconcile final workers' recreated folders, including saved paths from before a
restart. Unreadable roots are reported and skipped. Worker dispatch schedules
pending deletion without waiting for reconciliation or filesystem read slots.
Running workers and unrelated temp entries are kept.

Before deletion, Torii atomically renames the folder. Removal runs on a daemon
thread, does not follow symbolic links, and handles read-only and mode-000
folders. Worker completion, dispatch, startup, and shutdown do not await this
background removal. A later resume can create its fresh folder while the old
tree is removed. An indexed persistent queue limits each dispatch to 16 workers.
Folder indexes and process checks run off the event loop. Failed cleanup backs
off from 2 seconds to 300 seconds; problem records are throttled to once per
minute. There is no separate cleanup timer.

## OpenCode subscription workers

OpenCode workers stay behind `delegation.opencode`, which is off by default.
Add one subscription with `opencode.account` and
`{"alias":"example-host","provider":"opencode-go","model":"example-model"}`, then select it
with `opencode.use {"alias":"example-host"}`. Use an unprefixed model ID from the pinned
OpenCode 1.19.3 catalog. The subscription needs no host URL, context limits or token prices.
The owner writes the key to `~/.opencode-accounts/example-host/key`, owned by the service
user with mode `0600`. The generated config references that file.

With delegation on, `/accounts`, `accounts.list` and `account.show` show the
5-hour, weekly and monthly percentages left and UTC reset times. The usage
request also checks the key and subscription. Invalid keys and missing
subscriptions show an account problem instead of meters and fail the worker.
Subscription usage refreshes independently every five minutes and on owner account checks.

OpenCode 1.19.3 retries 429 responses itself up to five times and honors
`retry-after`. Torii pauses the worker only after those retries finish.
At a subscription limit, the worker waits with its native session and account intact.
Torii checks usage after the reported reset before resuming on that same account.
Without a reset time, Torii checks again within five minutes. The subscription never rotates
to another account. Removing the subscription account or switching delegation off leaves
parked subscription workers waiting until the owner stops them. Generic OpenCode host
accounts keep their existing setup.

## OpenCode parent recovery

An OpenCode parent keeps its original host error before compaction can replace
it. Lifecycle failures and problem details include the status, error code and
a short filtered reason. A provider's `weekly_included_input_token_limit_exceeded`
is a usage pause, even when OpenCode calls it `ContextOverflowError`. The pause
names the weekly input allowance and tells you to enable paid balance use in
the host subscription settings, if available, or wait for the weekly reset.
Torii waits until the captured reset time before rechecking on the same account,
model and native session. Without a future reset
time, it rechecks after five minutes. A credit top-up alone does not renew this
subscription allowance.

Other failed turns back off for 30 seconds, doubling to five minutes. An owner
message or changed account configuration ends that backoff. Saved unanswered
messages remain queued; repeated attempts do not append recovery callbacks and
restart notices. This state survives a service restart.
Before recovery, Torii confirms native receipts across this chat's retained
sessions. Answered rows stay settled after a restart or a session change.
Retries name inputs already recorded in the native session in one reusable
callback instead of inserting those inputs again. An account change keeps its
continuity notice and owner report.

A real context overflow starts one fresh native session after the old writer
exits. Its catch-up is limited to 8,000 characters, including open jobs and
previews of at most ten unanswered messages. The original saved messages follow
after the catch-up completes. Old native history stays in place. If the catch-up
or its resumed turn fails, the chat pauses with a notice. A new owner message
allows another attempt after the host problem is fixed. A known provider weekly
quota refusal instead waits for its captured reset without another fresh
session. If a catch-up host disappears or exits while Torii is offline, startup
reconciles its completion or pauses with a notice. New owner input can wake that
pause. Recovery does not change the selected provider, model or delegation policy.

## Worker process ownership on macOS

New OpenCode worker hosts on macOS run in a separate launchd job in the service
user's GUI domain. OpenCode's existing enabled switch gates this path. Each
host folder holds an atomic private plist and an atomic `owner.json` with the boot
identity, host unique identity, both coalition IDs and cleanup state. Private
launch input crosses a one-use local socket and never enters the plist.

Before provider execution, the host must be the only process in its resource
coalition and the kernel must reject a stale audit token. Failure stops launch
with `ownership_unavailable` and a plain error line. At completion or Stop,
the host first keeps the existing graceful process-group stop. It then freezes
same-uid coalition members, rescans for new members, and kills each by audit
token. Selection also requires a unique identity at least as new as the host.
Cleanup excludes the host itself. It has bounded rounds and a five-second
empty-coalition deadline. It saves the result before writing `exit.json`.

If the host dies without an exit record, the service uses the owner record to
clean its remaining processes before reporting host death and removing the job.
Startup recovery leaves running hosts alone and removes exited or dead host
jobs. A `job-booted-out` mark makes removal idempotent. A changed boot identity
marks ownership stale and sends no signals. Cleanup failures record the
`worker` / `ownership-cleanup` problem with the remaining count. They preserve
the run's result status and retain OpenCode launch scratch for investigation.
Only an `ok` cleanup allows OpenCode launch scratch removal.

Recovery records `worker` / `ownership-recovery` for a folder error and continues to the next host, so damaged folders do not stop startup or worker dispatch.
A bootout error records `worker` / `ownership-bootout` without changing a completed provider's result or scratch cleanup rules.
A failed bootout leaves `job-booted-out` unset for retry; recovery skips cleanup already recorded as `ok` or `stale` and keeps cleanup reports once per owner record.

Claude and Codex workers, parent hosts, Linux, and specs without the ownership
field keep the Popen path. Existing hosts
started before this change keep their original lifetime rules and are not
retrospectively swept. This is process cleanup, not a sandbox. Changed-uid
children, separate jobs created through launchd, XPC or application brokers,
and remote or server-side work are outside the guarantee. The Darwin metadata
ABI is private and can change. Per-host capability checks refuse unavailable
ownership; bounded cleanup reports failure for persistent processes or denied
signals. Unlimited fork storms and all kernel race schedules are not proved.

Run commands from this checkout. Use one service poller per bot. The foreground
service and the installed service (the macOS LaunchAgent or the Linux systemd
user unit) are alternatives.

## Group setup and credentials

Claude sign-ins stay in each profile folder. Torii never reads or passes Claude sign-in tokens.

The **Torii** topic holds setup controls and sign-in cards. Send the first
request there and choose a project name. Torii creates that project's topic.
Use **New project** in `/projects` for another project. Agents turn on after
Claude or ChatGPT connects.

Tap **Fill privately** on secret cards and send the value to the bot DM.
`/cancel` stops intake. Paste a Claude sign-in code in the topic of its card.
If deletion fails, delete it by hand.
Work belongs in group topics.

If the owner leaves, new work pauses while current sessions and delivery
continue. Rejoin to resume. If the bot is removed, add it back to the same
group as the owner. `setup.py --status` prints a recovery link.
`python3 -m coordinator pair --replace` moves Torii to another group. Projects
and native sessions stay on this computer. Missing Manage topics shows **Fix permissions**.

## Inspect local state

```sh
python3 -m coordinator status
python3 -m coordinator bot
python3 -m coordinator ctl list
```

`status` shows topics, tasks, and recent service workers without credentials.
The SQLite database lives at
`~/.local/state/telegram-agent-coordinator/state.sqlite`. The bot token lives at
`~/.config/telegram-agent-coordinator/bot-token`. Provider event files and
service logs live under the state directory. Keep tokens and transcripts out of
issues and shared reports.

## Enable execution

Each topic needs a project selected through **Link this topic** in `/projects`
or a local binding with `--enable`. The service must run without `--pair-only`. Running
`scripts/install-service.py` enables agents by default.
`--pair-only` is an explicit operator flag. Fresh setup holds work until Claude
or ChatGPT is ready through its execution latch. You do not need to reinstall after successful setup.

After setup, send `/ping` in your paired chat and confirm a reply. Service
registration alone does not prove Telegram connectivity. If no reply arrives,
run `python3 scripts/setup.py --status` and inspect the service logs.

```sh
./scripts/install-service.py --coordinator-model YOUR_MODEL_ID
```

Omit `--coordinator-model` to use the `sonnet` alias on a new install. Saved
model settings take precedence. Existing installs preserve their previous
implicit coordinator model. Keep the checkout and Python executable at the paths
used during installation. Reinstall after moving them. A sleeping computer does
not process messages. The macOS LaunchAgent starts at login; Linux uses a
systemd user service with linger.

To run a fixed commit instead of the development checkout, see
[Pinned release](release.md).

`service.restart` exits after the outbox drains and a private SQLite backup
succeeds. It interrupts no coordinator or worker process. The service refuses
a repeat restart within two minutes. On relaunch, queued workers remain queued.
The service reattaches to live provider hosts and reads events saved while it
was down. A finished worker becomes a normal `worker_result` message. A worker
whose host died without an exit record becomes `interrupted`, and the
coordinator receives its saved session and process context. It checks prior
effects before any continuation.

To stop the macOS service poller, use the following command. Detached provider
hosts continue running. The command does not remove provider history or runtime
records.

```sh
launchctl bootout "gui/$(id -u)/local.telegram-agent-coordinator"
```

## Update Torii

These steps cover the default macOS LaunchAgent installed from a checkout.
For a pinned release, use [Apply a release](release.md#apply-a-release).
Keep any custom plist settings separately before reinstalling. The installer
rewrites the plist, including the model, mode, environment, and checkout path.
It uses the default state directory, even if your shell sets `TORII_STATE_DIR`.
Use the pinned-release procedure for a custom service definition.

1. Stop submitting new work. Let current jobs and background tasks finish.
   Check `/health` and the local worker list:

   ```sh
   python3 -m coordinator ctl call workers.list
   python3 -m coordinator ctl call health.show
   ```

   To cancel a worker, run `python3 -m coordinator ctl call workers.stop worker=ID`
   with its actual ID. A stop request is queued. Confirm that the worker stops.
   Check queued workers and workers waiting for secrets or quota too.
   Let each coordinator finish its turn and its native background tasks.
   Idle coordinators wind down after one hour. Wait until `/health` shows no
   running agents before changing code. Stopping the poller alone leaves
   detached agent hosts running with their old code.

2. Read the installed checkout and interpreter paths without reading credentials:

   ```sh
   torii_plist="$HOME/Library/LaunchAgents/local.telegram-agent-coordinator.plist"
   torii_checkout=$(/usr/libexec/PlistBuddy -c 'Print :WorkingDirectory' "$torii_plist")
   torii_python=$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:0' "$torii_plist")
   git -C "$torii_checkout" status --short --branch
   "$torii_python" --version
   ```

   Confirm that this is your intended Torii checkout and Python 3.9+ interpreter.
   Preserve local changes before continuing. Do not reset a dirty checkout.

3. Unload the settled poller, then update the checkout:

   ```sh
   launchctl bootout "gui/$(id -u)/local.telegram-agent-coordinator"
   git -C "$torii_checkout" pull --ff-only
   cd "$torii_checkout"
   TORII_PYTHON="$torii_python" ./scripts/verify-local.py
   ```

   If the pull or verification fails, stop and inspect the failure before reloading.
   The offline baseline does not check Telegram connectivity.

4. Reinstall with the saved interpreter:

   ```sh
   "$torii_python" scripts/install-service.py
   ```

   This writes the checkout path and invoking interpreter, unloads any prior
   registration, and bootstraps the per-user LaunchAgent. Add `--pair-only`
   if that was your mode, or `--coordinator-model MODEL_ID` for a custom model.

5. Confirm the loaded revision and a reply:

   ```sh
   git rev-parse --short HEAD
   "$torii_python" scripts/setup.py --status
   tail -n 80 "$HOME/.local/state/telegram-agent-coordinator/logs/service.log"
   ```

   Find the new `service start commit=... code=... home=...` line and the
   `service run commit=...` line. Compare the commit and checkout path with
   the intended update. Then send `/ping` in the paired group and confirm its
   reply. A successful installer exit or a launchd registration alone is not
   proof that the update works.

## Uninstall Torii

1. Settle coordinators, native background tasks, and workers as in
   [Update Torii](#update-torii), step 1. Stop unwanted queued or waiting
   workers with `workers.stop`, and confirm their final status. Wait for idle
   coordinators to wind down. Confirm there are no running agents before
   unloading the poller or moving a checkout.
2. Read `WorkingDirectory` from the LaunchAgent as in update step 2.
   Inspect the paths you plan to move:

   ```sh
   ls -ld "$torii_plist" "$torii_checkout"
   git -C "$torii_checkout" status --short --branch
   launchctl bootout "gui/$(id -u)/local.telegram-agent-coordinator"
   launchctl print "gui/$(id -u)/local.telegram-agent-coordinator"
   ```

   The last command must report that the service is absent. If it still exists,
   inspect that service before continuing. For a pinned install, inspect its
   release and development checkout separately. Preserve shared or registered
   Git worktrees until you have handled their ownership.
3. Move only the inspected plist and ordinary Torii checkout to the macOS Trash.
   The following command refuses a missing target and adds a suffix when the
   Trash already contains the same name:

   ```sh
   python3 - "$torii_plist" "$torii_checkout" <<'PY'
   from pathlib import Path
   import shutil
   import sys

   targets = [Path(value) for value in sys.argv[1:]]
   if any(not path.exists() and not path.is_symlink() for path in targets):
       raise SystemExit('A target is missing. Inspect both paths before continuing.')
   trash = Path.home() / '.Trash'
   trash.mkdir(exist_ok=True)
   for path in targets:
       destination = trash / path.name
       suffix = 1
       while destination.exists() or destination.is_symlink():
           destination = trash / (path.name + '-' + str(suffix))
           suffix += 1
       shutil.move(str(path), str(destination))
   PY
   ```

   Keep `~/.local/state/telegram-agent-coordinator`, including the encrypted
   vault and its key, and `~/.config/telegram-agent-coordinator` by default.
   Preserve any external vault key too. Keep project folders, task worktrees,
   native account homes, and all Claude and Codex history. Removing saved state
   or credentials is a separate owner choice. Inspect each chosen target and
   move it to Trash too. Do not hard-delete it or include native history in
   routine uninstall cleanup. Torii's uninstall does not uninstall the native CLIs.

## Native CLI contract

Torii has no enforced minimum Claude or Codex version. `scripts/setup.py`
requires a successful `--version` check for at least one CLI. It also checks
that a Codex installation supplies an executable `codex-code-mode-host` where
that packaging requires it. A successful version check does not test all features.

Claude must support stream-json input and output, `--replay-user-messages`,
`--effort`, MCP tools, exact session resume, and `auth login --claudeai` with
`auth status`. Unsupported options or startup failures appear as setup or
provider errors. Install Claude from the [official setup page](https://code.claude.com/docs/en/setup).

Codex must support `app-server --listen stdio://`, its initialize handshake,
`thread/start`, `thread/resume`, `turn/start`, and `turn/steer`. Account setup
uses `account/login/start` with `type="chatgptDeviceCode"`, account reads,
and rate-limit reads. Worker goals require the `goals` feature and
`thread/goal/set`, `thread/goal/get`, and `thread/goal/clear`.
Secret workers use `config/read` before and after thread start or resume and
refuse effective configuration that defeats their launch controls.
These are runtime protocol checks, not one installer capability probe.
Install Codex from the [official CLI page](https://developers.openai.com/codex/cli).

The opt-in account-free parent contract in `tests/test_codex_parent_contract.py`
is pinned to Codex 0.159.2. It checks parent threads, steering, Torii MCP calls,
and transcript receipts against a loopback mock. It does not establish a
minimum version for sign-in, worker goals, or secret delivery. If your CLI
rejects a required option or protocol method, update it through its official
installation method and repeat the failed step.

## Message reactions and job reports

Torii selects reactions in the service and MCP handlers. The coordinator never
chooses an emoji. Torii has at most one reaction per owner message.

| Event | Reaction |
| --- | --- |
| A job is created from an owner message with a Telegram ID | 👀 |
| A plain owner message reaches its coordinator, the parent makes two tool calls or delegates, and no report was queued in that channel since hand-off | 👀 |
| A worker starts with `work=dev` | 👨‍💻 |
| A worker starts with `work=research`, the default | 🤔 |
| The job becomes done | 👌 |
| A turn that gave 👀 ends without a job created from that message | Remove the reaction |
| The job is dropped | Remove the reaction |
| The job is reopened | 👀 |
| Work waits for the owner | Keep the reaction |

A plain turn is an owner message that is not a job's origin. Torii counts
live Claude parent `tool_use` blocks from each message's native hand-off,
including a steer into a running turn. Two calls give 👀. A single `Agent`,
`Task`, `Workflow`, or `mcp__torii__workers_spawn` call also gives 👀.
`mcp__torii__telegram_send` does not count. Replayed history and subagent
assistant events do not count. Elapsed time alone gives no reaction.
A queued message has no count until hand-off. Reports queued before hand-off
or in other channels do not suppress 👀. A report queued in the same channel
after hand-off suppresses it, even before delivery. Counts persist through
recovery. Once a job exists for a message, job events own its reaction.
The latest started worker wins. Attaching an existing worker after a
service restart does not count as a new start.

`reaction_desired` stores the emoji or null on the target row in `messages`
or `outbox`. A reaction reply keeps `telegram_message` null and stores its
target Telegram number in `reacted_to`. The delivery loop skips message rows
without their own Telegram number and applies desired reactions to the
original message row or bot outbox row. The existing reaction
loop calls `setMessageReaction` to replace the bot reaction, or sends an empty
list to remove it. `reaction_sent` stores the last confirmed result. Failed
calls keep the existing backoff and problem records. A 400, a 403, or five
failed attempts disables delivery for that message.

Existing jobs have a null `origin`. Until a parent loads the new MCP code, its
old tools can still create jobs without origins.

Each parent launch fingerprint includes a stable hash of Torii's MCP tool
names, descriptions, and input schemas. After a service restart onto this
revision, each existing parent relaunches once when idle to load the new tools.
An active turn settles first. The relaunch preserves the native session ID,
working directory, and history. Unchanged tools do not cause another relaunch.

Report on a job with `telegram.send task=NUMBER`. The service quotes the job's
owner origin, or the message that owner reacted to, in that channel. A job without a valid origin sends without a
quote and reports `quoted=false`. An explicit `reply_to` takes precedence.
Torii sends without a quote when `reply_to` names a message in the same channel
without a Telegram ID. It reports `quoted=false`. Torii refuses unknown message
numbers and messages from other channels.
See [Control API](control-api.md#persistence) for the number and reply checks.

Torii requests `message_reaction` updates in its long poll. It forwards only
reactions the paired owner adds in registered chats. It resolves the topic
through `outbox`, then `messages`, by chat and Telegram message number because
the update has no topic ID. Removals and other people's reactions are ignored.
Unknown messages produce a log line and no problem record.

The service saves each update as an owner reply through the normal reply
path. Its text is `Replying to: <quote>` followed by `Reacted 👍` on the next
line. The quote collapses whitespace and has at most 300 characters. Several
added emoji appear space-separated. Custom emoji appear as `(custom emoji)`,
and paid reactions appear as `⭐`. The reply starts or steers the parent like
any owner message. Jobs created from it and the turn rule react to the
original message. Job reports quote that same target.

When a parent completes a turn normally without sending a message, Torii
forwards its nonempty final text once. Claude and Codex require an owner
message in that turn. OpenCode also forwards turns without owner messages.
For every provider, Torii skips this forwarding when all owner messages in
the turn are reactions. A turn with both a reaction and a real owner message
keeps normal forwarding. Reactions still reach the agent so it can act on
an approval or an answer. A reaction alone otherwise needs no reply.

[Telegram requires administrator status](https://core.telegram.org/bots/api#update)
for these updates. At startup, Torii calls `getMe` and `getChatMember` once per
linked chat. A non-administrator records `reaction-admin-required`. A failed
check records `reaction-admin-check-failed` and lets startup continue. Give the
bot administrator status in each linked chat to receive owner reactions.

## Transfer an existing native session

1. Inspect the native session, its background agents, and its shell tasks.
2. Record its exact session ID, provider, working directory, and owner PID.
3. Bind it without `--enable` while its current owner still runs.
4. Wait for its work to settle and exit the old writer. Keep unrelated work open.
5. Confirm that no process writes the session. Bind again with `--enable` and
   the same saved fields.
6. Send a read-only continuation request and confirm the exact session ID.

Replace every placeholder in this example.

```sh
python3 -m coordinator bind --topic=-100123:4 --cwd=/absolute/project --name=Project --provider=claude --session=SESSION_UUID --source-pid=OWNER_PID
```

Never restart the old writer while Torii owns its session.

## Telegram controls

The paired owner can use local commands before a model is connected. The service
checks numeric owner and group IDs. Telegram usernames are display text.

| Command | Effect |
| --- | --- |
| `/accounts` | Show usage, sign-ins, resets, and models. |
| `/projects` | Show projects, new projects, and this topic's folder. |
| `/tldr` | Catch up on this topic since your last message. |
| `/health` | Show running agents per channel, parent state, system pressure, and subscription use. |
| `/secrets` | Show stored keys with buttons to rotate, revoke, or ask again. |
| `/ping` | Confirm the bridge receives messages. |
| `/help` | List the commands without buttons. |

These are the seven commands in the Telegram command menu. They take no
arguments. Old account, project-list, and secret forms open the matching card
with `Commands take no arguments now. Use the buttons below.` They run no action.

The following forms still work but stay out of the command menu:

| Command | Effect |
| --- | --- |
| `/goal CONDITION` | Keep the agent working until CONDITION holds. It has one line in `/help`. |
| `/setup` or `/start` | Show setup status in Torii, or link a project in another topic. |
| `/project new NAME` | Create a project and topic. Use this form in General, where input prompts are unavailable. |
| `/project folder /absolute/path` | Change the bound project folder when no job is open. |
| `/service restart REASON` | Queue service exit after outbox delivery. |
| `/update` | Review and apply Torii and agent CLI updates. Off by default. |

Models, Codex delegation, and usage policy are under **Models & Codex ›** in
`/accounts`. Channel enable and disable stay local controls. Use
`python3 -m coordinator ctl call --topic=CHANNEL topic.enable` or `topic.disable`,
where CHANNEL is a channel ID or a unique channel name. Per-account disable and
cached-limit clearing also stay local controls. They have no Telegram buttons.

In `/accounts`, **Add Claude** starts the native Claude sign-in for a new
profile. It asks for no name. The service creates a new folder
`~/.claude-accounts/.torii-HEX`, runs `claude auth login --claudeai` with
`CLAUDE_CONFIG_DIR` set to it, and posts a sign-in card with the link.
To finish the sign-in:

1. Tap the link on the card and sign in to the Claude account to add.
2. Copy the code it shows.
3. Paste it in the topic of the card as one message.

While the sign-in waits, Torii takes the owner's code-shaped message in that topic,
deletes it at once, and hands the code to the waiting process on stdin. If the
delete fails, Torii still uses the code and asks the owner to delete the message.
The code never enters the vault, the log, the state database, or a reply. If that
topic is disabled, other text gets a pointer back to the card and is not passed
to an agent. In an enabled topic, other text is handled as a normal agent message.
The code works once and only with the login that showed the link. A code sent to
the bot DM is deleted, and the reply points back to the card.
After `claude auth status` reports the login, Torii shows the login email as the
account name. It keeps the profile key and `.torii-HEX` folder internal.
Registered signed-out accounts stay in `/accounts` with their last email, or
their account name if no email is known. Discovery never adopts an unfinished
sign-in.

Tap **Sign in …** on a signed-out row, then **Sign in**. Sign in with the same
provider account to keep its account name, pick, settings, and native history.
Torii points the entry at the new sign-in folder and keeps the old folder in
place. **Add Claude** or **Add ChatGPT** also restores a signed-out entry when
the email matches. A different, new email adds a new account and leaves the
original signed out. An email already signed in adds no duplicate.

The signed-out account card also has **Remove**, followed by a **Remove**
confirmation. Remove takes the entry off Torii's list and touches no files.
Torii remembers its folders and does not add them again through discovery.
Only signed-out accounts can be removed. Finish or cancel a sign-in targeting
that account first. A signed-in account that is off has **Turn on** instead.

A wrong or expired code gets a new card, up to three attempts. A card expires
after 10 minutes.
A failed or expired sign-in shows **Try again** on its card. **Cancel sign-in**
on `/accounts` or the sign-in card stops the sign-in. The private `/cancel`
also stops it. A stopped sign-in removes its new folder only when the folder is
empty. One sign-in runs at a time. A restart stops an open sign-in. Start again
after the restart.
While a sign-in runs, `/accounts` shows **Cancel sign-in** in place of
**Add Claude** and **Add ChatGPT**. The buttons edit the accounts message in
place. The sign-in card is its own message.
Each Claude profile keeps its own sign-in, and Claude Code refreshes it.
The service never reads or passes Claude sign-in tokens. **Use … for Claude** in
`/accounts` saves the owner's pick. New work uses that account whenever it has
room, otherwise the eligible account with the soonest weekly reset, with alias
as the tie-break. The pick stays preferred during a pause and takes work again
when available. **Automatic** clears the pick. Eligible running parents stay put
after clearing it. A running parent moves to an available pick or off an
ineligible account. The causes are 98% usage
in a 5-hour, all-model weekly or Fable weekly meter, a local reserve, a hold,
a signed-out profile, or an account the owner disabled. The parent waits for
its turn and background tasks to finish, then restarts under the next eligible
account and resumes the exact native transcript without copying history.
A worker switches accounts when Claude rejects it for usage.
A usage rejection mid-turn restarts the parent at once and asks it to check
the interrupted messages. Background tasks stopped by that restart have lost
results. A callback names them and asks the parent to check their state.
Stale snapshots remain eligible unless a meter or hold blocks them.

Picking a Claude account requests a live usage check. The service trusts that one
reading even if usage is lower within the same window, and clears its hold when
all meters have room. After spending a saved reset on Claude's site, tap the
paused pick's **Use … for Claude** button to check it again. The immediate reply
confirms the pick; the fresh result reports room or the remaining limit and
fallback account in the same chat, or the main chat when the origin is unknown.
The main chat receives one notice per all-model weekly or Fable weekly pause of
the pick, with the return time and account taking work meanwhile. There is no
notice for a 5-hour pause. Ordinary usage checks keep the existing cache rules
for threshold holds, local reserves and accounts already released from a hold.
An active quota hold can trust an error-free native reading for the window named
by the refusal. Other windows keep the existing cache rules. If the refusal names
no window, the hold can trust the whole reading. The hold releases only when every
known window is under the account's switch point after these rules, including a
local reserve. The card shows that usage and the next real work can try the
account. A usage refusal restores the hold until that refusal's reset and rotates the work. The
release reading is saved on the block for the previously full windows, or all
reported windows when none was full. Another release requires a utilization
change or a reset time change of at least five minutes in one of those windows;
smaller reset differences count as the same window. Each release saves its
reading again. The refusal's hold also expires at its own reset, or after five
minutes when Claude supplied no future reset. No extra probe or model call
tests the account.

Torii cannot spend Claude resets or see their banked count. The owner spends
them on [Claude's usage page](https://claude.ai/settings/usage). Check which
Claude account is signed in to the browser before spending one.

In `/accounts`, **Add ChatGPT** starts a Codex device-code
sign-in in a new home `~/.codex-accounts/.torii-HEX`. The card shows a link and a
one-time code; enter the code on the ChatGPT page within 15 minutes. The same
**Cancel sign-in** and **Try again** rules apply.

Each parent launch prefers the owner's available Claude pick, then another
eligible Claude account, then the active ChatGPT account. Use `model.parent` through
[local controls](control-api.md#setup-and-settings-operations) to select Claude
or ChatGPT and its model for all coordinators. A selection applies after the
turn, pending receipts, and background work settle. Selecting Claude keeps the
ChatGPT fallback: if no Claude account can take work, the parent uses the active
ChatGPT account and the configured Codex model while Claude is unavailable, then
returns to the selected Claude model on the owner's available pick, otherwise
another eligible Claude account, once work settles. A ChatGPT parent selection
uses only ChatGPT. If neither allowed provider can take work, messages stay saved,
Torii sends one notice, and the parent retries at the earliest reset.
Without a parent selection, a running parent keeps its provider
and returns to Claude at the next launch after Claude recovers, including after
its one-hour idle wind-down. Switching provider starts a fresh native
conversation with task and message records for continuity and a notice in that
topic. ChatGPT parent accounts never switch automatically, even with worker
auto-switch enabled.
At the active account's limit, the main chat saves messages and pauses until
the owner picks another account with `/accounts` or the limit resets. `/health`
names the parent provider or shows its paused state. `/tldr` uses an ephemeral
read-only ChatGPT thread when Claude is unavailable. Saved secrets and worker
goals work with both providers.

The ChatGPT parent exposes Torii MCP tools directly with
`mcp_servers.torii.omit_tools_from=["deferred","code_mode"]` and
`default_tools_approval_mode="approve"`, under `approvalPolicy="never"` and
`sandbox="danger-full-access"`. These are launch overrides, not account config
edits. On Codex 0.159.2, the default model sends tool schemas in Responses Lite
`input` items of type `additional_tools`. The opt-in contract test checks the
advertised tool names and an actual write to an isolated outbox.

Torii's Codex worker and ChatGPT parent launches always hide the installed-skills
list with `skills.include_instructions=false`. Skills stay installed. A short
Skills note ends the worker instructions or parent prompt. It tells the worker
to read a skill's `SKILL.md` before use, find named plugin skills, and list the
repository, user, and system skill folders for relevant skills.

Automatic ChatGPT switching for Codex workers is off by default. Codex work then
runs on the account you last picked. With no pick, Torii uses the signed-in
default home, else the first signed-in account in alias order. Adding an account
keeps a signed-in account in use. Tap **Use … for ChatGPT** in `/accounts`, or
ask the coordinator to switch. These buttons appear only when auto-switch is off.
Torii sends no low-usage warnings for Codex or Claude. The owner can use a
separate watcher. If an account rejects work and work cannot continue, Torii
sends one hard-stop notice per account and window to `coordinator_home_topic`.
It sends no account-limit notice to a project chat. With no enabled home chat,
Torii queues no notice and uses no fallback chat.
With no known reset, repeated checks share one notice until work succeeds.
After a rejection, Codex workers wait as `waiting_for_quota` and resume on the
active account after a switch or the reset.
`/accounts` shows **Turn on ChatGPT auto-switch** or
**Turn off ChatGPT auto-switch**. With it on, new Codex launches select an account
under 95%. A running worker resumes the same thread on another account after a
usage-limit rejection. When every Codex account is limited, the worker waits as
`waiting_for_quota` and resumes automatically when an account becomes available.
Usage snapshots, account blocks, and automatic switching continue without
low-usage warnings.
Automatic usage fallback and recovery reports are also silent. A blocked worker
reports at home instead of sending a usage callback to its project coordinator.

Signed-in ChatGPT accounts with banked resets have **Reset … (N)** in
`/accounts`. Tap it to see the count, current limits, and first expiry when
Codex supplies one. **Spend reset** confirms an irreversible spend. Torii spends
one only when the account is limited, ordinary usage is refused, or an ordinary
5-hour or weekly meter is at least 95% used. Below that rule, the card has only
**Back** and keeps the resets banked. The same rule applies to Telegram, the
local CLI, and agents. Torii checks live usage again before spending. It spends
the earliest-expiring reset when Codex supplies and supports that choice.
Otherwise Codex picks. The result arrives as a new message. Torii never spends
a reset automatically.

`/accounts` lists registered Claude and ChatGPT accounts, including signed-out
ones. Percentages show usage left. Claude has 5-hour, week, and Fable windows.
ChatGPT has 5-hour and week windows. A `picked` tag marks the owner's Claude pick.
A `next` tag marks the next Claude account,
and `active` marks the ChatGPT account in use. Limited rows say `back in X`.
Stale rows say `stale since` with a time. Local per-account reserves appear as
`reserve N%` and have no setting button. Signed-in rows need no separate card.

**Models & Codex ›** opens **Worker model**, **Codex model**, and
**Coordinator model**. It also has **Turn Codex off** or **Turn Codex on**, and
**Usage policy**. **Opus 5.5** (`claude-opus-5-5`) is an optional model preset.
New installs use the `sonnet` alias for Claude coordinators. The service reads
`coordinator_model` at each coordinator launch. When clear, it uses an explicit
`--coordinator-model`, then the install's preserved fallback or the new default.
Worker and Codex model changes apply to future dispatches. Coordinator model
changes apply at its next launch. A saved `model.parent` provider and
model selection takes precedence over these model settings and applies after the
turn, pending receipts, and background work settle.

**Usage policy** edits `USAGE.md` in the state directory. Keep owner-specific
working and shipping rules there. The default has no task-to-provider routing.
Choose your own rules or adapt the [example policy](usage-policy.example.md).
All parent providers receive the full policy. Workers omit each
`## Coordinators` section through the next `## ` heading or the end of
the file. See [Coordinator and delegation](orchestration.md) for the heading rules.

`/projects` lists project folders and their linked topics. **New project** asks
for a name as a reply to its card, then makes a Git folder and opens a topic.
**Projects folder** sets the folder for future projects. In a linked topic,
**This topic ›** shows its folder and has **Change folder** and **Open jobs**.
An unlinked project topic has **Link this topic** instead. The Torii control
topic has neither topic button.
The coordinator creates a worktree when it commits to a task. A task may have
several agents in its worktree. The parent coordinates shared edits.

Torii sweeps closed job worktrees on startup and every hour. By default, each
pass checks at most 10 eligible folders and saves its progress. After 24 hours
without an update, a clean done or dropped job's copy and its Git registration
move to the Trash. Ignored files go with the copy. Space returns when the Trash
is emptied.

Immediate permanent deletion ships off. To enable it, run the local CLI from
the Torii checkout:

```sh
python3 -m coordinator ctl call cleanup.delete enabled=true
python3 -m coordinator ctl call settings.get
```

`settings.get` shows `closed_copy_delete: True`. Set `enabled=false` to restore
the default policy. The switch is local CLI only and adds no MCP tool.

With deletion on, closing a done or dropped job checks its copy immediately.
The close answer says whether the copy was deleted or kept, and why. Uncommitted
parent changes or untracked parent files keep it until they are committed or discarded. An
unfinished worker for the job, or any unfinished worker whose working directory
is inside the copy, also keeps it. Completion of the last such worker retries
the check. Startup checks every closed job. The existing hourly sweep catches
copies that become clean through manual commits or discarded changes. Enabled
sweeps have no 10-folder limit and add no timer.

Every check must pass. The saved path must identify the original real task
folder. Git must have exactly one unlocked registration on the saved job branch,
matching Git data, no operation in progress, and no tracked or untracked parent
changes. Submodules require a durable source salvage record before retirement.
The record includes binary diffs, untracked files, and verified bundles of commits
unreachable from remote refs. Preservation failure or changed source keeps the copy.
Deinitialized or orphan submodule metadata stays in place. See
[Worktree retention controls](worktree-retention.md) for salvage artifacts and the
opt-in external worktree sweep. A changed folder inode keeps the copy. Ignored entries do not
block deletion. Every branch, commit, and shared stash stays.

Under the repository lock and a Store transaction, Torii renames the proven
copy into `worktree-deletions` in its state directory on the same filesystem.
It moves only that copy's Git administrative directory there too, freeing its
branch. The close answer waits for these renames, not for byte removal. One
background consumer removes staged trees through `host_os.delete_tree_background`.
A child process uses directory descriptors, never follows symbolic links,
restores owner access to read-only and mode-000 folders, and rechecks their device
and inode after changing permissions. It reports removal counters between small
chunks. A slow filesystem call stays outside the service event loop. Cancellation
requests a graceful stop, then kills an unresponsive child within bounded waits;
no default-executor thread delays Python 3.9 shutdown. Neither the copy nor its
Git data goes to the Trash.

A recovery manifest in each staging folder records the original task, path,
branch, Git data inode, ignored top-level names, and removal counters. Recovery
finishes a crash between the two renames before a reopen reuses the branch.
The next cleanup pass and service startup finish staged bytes, even if the
switch has since been disabled. The Store records whether a copy is kept,
staged, or gone, with the reason for a kept copy. Each kept copy logs its reason
once per trigger. A completed deletion logs the job, folder, removed bytes and
files, and ignored top-level names. A failed staged deletion saves its progress
and reason, records a throttled problem, and lets other copies finish. Event
passes and service startup skip that failure; the hourly backstop retries it.
Invalid recovery manifests are reported and skipped. Git status warnings and
skip-worktree or assume-unchanged entries keep a copy because they can hide edits.

The task retains its saved worktree path. When the job reopens, `worktree.create`
or its next `workers.spawn` recreates the copy at that path from the kept local
branch and HEAD. Steering a finished worker also restores a missing copy before
resuming its saved session. This does not clone the repository.

Typed commands send a new card. Taps and input replies edit that card in place.
Buttons that ask for text, such as a model ID, a project name, a folder path, or
the usage policy, take only a Telegram reply to their card. A plain message in
the topic goes to the agent as usual.
**Back** returns to the parent card. The accounts, projects, and secrets lists
have no Back button. Old buttons expire when another card opens in that topic.

## Credentials

Torii asks for values directly in the private chat, one credential at a time.
**Fill privately** on an armed card opens
that chat. On a waiting card, it starts the private prompt and opens the chat.
If another ask is taking a value, finish or cancel that ask first.
After an armed ask expires, prompting pauses. Tap **Fill privately** on a
waiting card to continue, or send a bare `/start` to prompt the oldest waiter.
Telegram sends `/start fill` from the link that opens the private chat.
Torii deletes this command and keeps the armed ask and its prompt unchanged.
Other `/start` parameters show help without arming an ask or lifting a pause.

A task that needs a credential asks for it by name with `secret.ask`. The
service posts an Envelope card in the topic. To give the value:

1. Tap **Fill privately** on the card within 10 minutes.
2. In the private chat with the bot, send the value as one message. Use one line
   of 1 to 4096 printable characters that does not start with `/` and is not
   only digits.
3. Read the reply: `NAME stored. Length N, fingerprint F.` The service deletes
   your message and marks the card Filled.

Never paste a value in a topic. The service deletes a reply to an open card, but
it cannot detect a value in an ordinary topic message. If the service reports
that it could not delete a message, delete it yourself and rotate the
credential. `/cancel` in the private chat, or **Cancel** on the card, closes an
open envelope.

`/secrets` lists each name and its state without values. Tap a name to open its
card. **Rotate** asks for a replacement and keeps the stored value until you
fill the new envelope. **Revoke** opens a confirmation. Confirm with **Revoke**
to erase the stored value. Workers already running keep their copy until they
exit.

An expired Envelope card says it expired and offers **Ask again**. The button
posts a fresh card in the original topic with the same name, reason, consumer,
and job. The expired card then says `Expired. Asked again.` and has no buttons.
This tap also clears the pause and prompts the oldest waiting ask, unless another
ask is taking a value.

In `/secrets`, an expired, cancelled, or revoked request has **Ask again**.
It reopens the request with the same reason, consumer, and job. If a previous value remains
stored, the card also has **Revoke**. An open request points to its waiting
envelope card. Only one envelope per name can be open. Rotate and Ask again
post the envelope in the topic where you tap. Workers read secrets when they
start, so no Torii restart is needed.

Declare the names on the task before a worker starts:
`secrets=NAME` or `secrets=ENV_NAME=VAULT_NAME`. A worker for that task gets the
values in its environment only. Torii scrubs known literal and JSON string
forms from captured output, with exceptions described below.
Both Claude and ChatGPT workers receive the declared values. The parent receives
names and metadata only, and `secret.ask` remains parent-only. Each worker has its
own native process. The host passes values through a private stdin pipe; values
never enter argv, the host spec, configuration, protocol parameters, prompts,
instructions, or goal objectives. Reattachment keeps the original private process.
Account transfers reload the job's declarations into the replacement process only.

A stored key works for any job that declares it, including
`ENV_NAME=VAULT_NAME`, regardless of which job asked for it. A worker also waits
for another job's live card and starts when the owner fills it.
`/secrets` and `secret.list` show distinct jobs from worker use records, most
recently used first, with each channel name and its job number, such as
"Workshop job 40, Torii job 362". They show up to three jobs and a count of the rest
on one line. `secret.list` also returns each job's global `id`, channel id
(`topic`), channel name (`channel`) and channel-local `number` in its `jobs` list.
The key's card shows **Jobs** and keeps the asked-for job under **For**.
A key name holds one value, and a new fill replaces it for every job, so
different accounts need different key names.
**Revoke** removes the key for every job; running
workers keep their copy until they exit.

Secret-bearing Codex workers disable `shell_snapshot` and `shell_snapshot_v2`,
set `memories.generate_memories=false`, and set `otel.tool_result.max_bytes=0`.
Native telemetry response and assessment logging stay off.
They also disable the memory feature, native multi-agent tools, and dedicated memory tools so those
tools cannot pass a value to another native thread or shared memory file.
Values-free shell policy overrides force inherited environment variables through
default exclusions and user filters. Before a thread starts or resumes, Torii
checks the effective configuration and refuses controls that defeat delivery or
configured shell values that replace a declared name. Runtime-control and
Codex-stripped names are refused. Reattachment checks the saved launch controls.
Safety controls stay enabled for later resumes after declarations are cleared,
so the same native thread remains excluded from memory generation.

Both native providers retain raw tool output in the requesting worker's own
native history. A command that prints a value can leave it in that history.
Each installer must decide whether native history retention is suitable for the
credentials and providers they use before declaring secrets on jobs.
Torii applies its scrubber to captured results, events, journals, logs, and
Telegram output. The event
scrubber preserves JSON object keys and canonical UUID values under `session_id`,
`thread_id`, `threadId`, `id`, and `uuid`, even in nested tool data. A declared
value in one of those positions can remain in the capture. Encoded values and
native tools, transcripts, files, and provider requests are outside a universal
filter. The scrubber does not rewrite native history.
Workers must use values through the environment and report only whether
a variable is set and its length when verifying delivery. The coordinator compares
that length with `secret.list`.

The vault lives in `<LIVE_STATE_DIR>/envelope/` (`master.key` and
`vault.enc`). Restart backups exclude both files. If `master.key` is lost, every
value is lost, and the service refuses to create a new key while `vault.enc`
exists. Move `vault.enc` aside. In `/secrets`, tap each name and **Rotate** or
**Ask again** to fill it again. To keep the key outside the state directory, set
`TORII_VAULT_KEY_FILE` in the service manager's environment. That is a LaunchAgent
plist change on macOS or a systemd user unit change on Linux.

This release supports macOS and Linux. The vault uses the file backend, POSIX permissions,
atomic writes, and the Python standard library. The service uses a launchd
LaunchAgent on macOS or a systemd user unit on Linux, both installed by
`scripts/install-service.py`. Use macOS FileVault or Linux full-disk encryption
for disk protection at rest. For stronger isolation, run the service as its own OS
user. Platforms other than macOS and Linux are unsupported in this release.

## Images, files, and replies

Send a JPEG, PNG, WebP, or GIF photo or document with instructions in its caption.
The limit is 20 MB per image, the most Telegram lets a bot download. The service
saves images under
`images/message-ID/` in the private state directory. If an image cannot be
saved, the message text still reaches the coordinator, with a note that the
image could not be downloaded. The owner gets one reply that says the text went
through without the image and asks for the image only. Each album message keeps
its own images, so one failed image does not drop the others.

On macOS, images larger than 5 MiB or with a side longer than 2,576 pixels get
a JPEG copy for agent image readers. Torii uses `sips` to cap the copy at
2,576 pixels on its long side and 5 MiB. The prompt lists the copy and names
the original full-resolution image beside it in `images/message-ID/`.
The original stays unchanged. If conversion fails, the prompt uses the original,
and Torii records one problem row without blocking the message.

Torii retries a download up to three attempts in total, waiting 1 s and then
4 s. It retries network errors, timeouts, HTTP 5xx, and 429; for 429 it waits
the `retry_after` time when the API gives one of 30 s or less. It does not
retry an invalid or oversized file or another 4xx response. Each failed attempt
writes one log line with the attachment and message row ids, the error class,
and a reason code; the line never contains the bot token, the file URL, or the
Telegram file path:

```
download failed attachment=41 message=97 attempt=1/3 class=TelegramError reason=file-download-timeout retry=yes
```

Send any other file as a document: Markdown, PDF, JSON, CSV, zip, audio, or
video. Audio, video, voice notes, and animations are also accepted. The limit is
20 MB per file, the most the Telegram Bot API lets a bot download. The service
downloads each file into `files/message-ID/` in the private state directory,
with owner-only permissions. The file name is a safe basename of the Telegram
name that keeps the extension; a second file with the same name gets a `-2`
suffix. The coordinator prompt gets an `Owner-provided files for this message:`
block with each path, mime type, and size, and a hint to read it with the Read
tool. If a file is larger than the limit or its download fails, the owner gets
one reply that names the file and the limit. The message text still reaches the
coordinator, with a note that names the missing file.

Each message of an album keeps its own row and receipt, and the feed sends the
messages in order. A download failure does not rerun worker execution.

Reply to a coordinator question through Telegram Reply. The feed includes a short
quote of the replied-to message when it is available. It still sends every owner
message individually. There is no held-job or batch-resume command.

## Problems and logging

The service writes every line to `logs/service.log` under the state directory.
The file rotates at 5 MB and keeps 5 backups. Each failure also gets one row in
the `problems` table of `state.sqlite`, so you can audit failures after the log
rotates.

### Finished host logs

Torii always removes a finished worker's `events.jsonl` and `writes.jsonl` after
it saves the result and settles pending steer receipts.
If a worker result has `success=false`, has a `failure_code`, or has status
`interrupted`, Torii keeps the whole host folder until the job round closes as
`done` or `dropped`, or a newer round opens. Cleanup then follows the normal
process, receipt, and worker-temp checks.
The host must record its exit or a verified failed launch, and its host and
process group must have ended. A host that died without an exit record can also
qualify when both recorded host and child PIDs are dead and the child process
group is absent. Any recorded process ownership cleanup must succeed or belong
to an earlier boot. A spec without process records or explicit failed-launch
proof stays intact; absence of an exit record alone never proves completion.
Torii keeps `stderr.log`, which the worker result names as `log_path`, and
the launch and exit records for other workers when `close_clears_space` is off.
After the job is done or dropped and all its workers are finished, Torii removes
those remaining folders too. If disposable worker temp cleanup still needs paths
from a launch spec, the folder stays until that cleanup finishes; journals can
already be gone. Temp cleanup completion requests the remaining host deletion.

A later steer resumes the exact native session in a new host. It does not need
the old event log. Codex reads historical secret names from retained specs.
Before deleting those specs, Torii saves the names in the Store so a reopened
job still strips them from its next launch environment. Native provider history
and account folders remain untouched.

Torii removes an ended parent host folder when no saved parent host points to it.
It keeps current parent hosts, running and reattached workers, hosts with pending
receipts or missing results, and unknown host kinds without a Store worker mapping.
Missing process proof or damaged ownership records keep the folder for investigation.
There is no age rule or periodic scan of finished workers.

Worker completion, receipt settlement, parent replacement, and job closure
request background passes. Startup also scans every existing host folder, one
at a time, saving its host index in batches of at most 32 entries. An indexed
Store table tracks retained worker host names, so later
worker and job events inspect their own folders without a hosts glob per worker.
Gone folders leave the index. Startup ownership recovery runs in a stoppable
child when needed, rather than scanning and recovering every host before startup.
Worker completion, launch scheduling and steers never await deletion.
If an eligible host publishes its exit record before its process stops, Torii
registers a one-shot kernel process-exit notification and checks its state again.
The notification requests only that host's next pass, which repeats the Store
and ownership checks. It uses kqueue on macOS and pidfd on supported Linux
kernels, including for processes reattached after a service restart. No timer
or waiting thread scans final workers. Stopping the service closes the
notifications; startup reconstructs them from host records.
If a child process group survives its leader, Torii watches its remaining
members. Two complete, identity-checked terminal rosters can prove that a group
of unreaped zombies has ended. macOS enumerates only that process group; Linux
uses bounded `/proc` PID metadata and requires readable pidfds for zombies.
Both scans yield between small batches. Missing native notification support,
unreadable metadata or an incomplete roster retains the folder and uses the
same durable back-off as other cleanup failures.
Each pass logs folders and logs deleted, file bytes freed, interrupted
deletions completed, and kept folders counted by reason.

Deletion first renames each target atomically into `hosts/.deleting` in the same
state directory. Host and worker-temp cleanup share the detach and safe-directory
primitives in `coordinator/worker_storage.py`. Host cleanup uses that module’s
chunked coroutine. The rename and deletion both use held directory descriptors,
so replacing a worker or quarantine directory with a symbolic link cannot
redirect them. Deletion unlinks symbolic links without following them and makes
read-only directories writable. It yields between
batches of at most eight traversal steps and stops promptly with the service. It does not use an executor
thread for recursive deletion. The next enabled start finishes interrupted
deletions. A reusable four-byte `host-log-marker.json` payload in the state root
is flushed off the event loop and linked into finished folders. It rotates at
4,096 links; existing markers retain their own links to the old payload.
Cancellation settles only the current payload write and staging link before
stopping. Torii rechecks the worker and receipts before
publishing `logs-deleted.json`, which makes host readers treat removed journals
as an ended process without attempting replay. An interrupted staging marker
does not hide journals from receipt readers. Deleting a host folder unlinks its
marker without affecting other folders. The tiny current payload remains for
reuse. Hosts on another filesystem use a separately flushed marker instead.

A failed delete records `hosts/log-delete-failed`, throttled to one problem per
minute. Its retry deadline and attempt survive restart in the Store. Retries
start after 60 seconds, double up to one hour, and wake only for their next due
time; worker passes do not retry them. A successful delete clears its retry.

### Native session ownership

Native session deletion is off by default. Enable it from the local CLI with
`python3 -m coordinator ctl call cleanup.sessions enabled=true`.
Use `enabled=false` to stop before the next deletion. `settings.get` exposes
the `session_delete` setting. This switch is independent of the other cleanup
switches. Deletion is permanent and does not use Trash.

Torii records each native session's provider, ID, launch home, session root,
and worker or coordinator owner. A worker's session becomes eligible only when
its job round is done or dropped, every worker in that round is final, and
their results and result messages are stored. Open, waiting, and interrupted
jobs retain their sessions. Reopening a job starts a new round and can resume
an intact session from an earlier round. Sessions planned for deletion or
already deleted cannot resume.

A coordinator replacement counts only after the native session acknowledges
startup. OpenCode also requires MCP readiness. Each chat retains its current
and previous confirmed native sessions. Resuming the same session, including
an account switch, does not count as a new generation. Old hosts must finish
before their sessions can be removed.

The native storage mappings are:

| Provider | Session storage |
| --- | --- |
| Claude | `<launch-home>/projects/<project>/<session-id>.jsonl` |
| Codex | `<launch-home>/sessions/YYYY/MM/DD/rollout-*-<session-id>.jsonl` |
| OpenCode | `<state>/opencode/workers/<worker>/data/opencode/opencode.db`, or `<state>/opencode/parents/<chat-hash>/data/opencode/opencode.db` |

Codex's shared session folder is resolved and pinned at registration. File
cleanup matches IDs and paths without reading transcript contents. Each file
deletion is journaled with its device and inode before removal. A changed file,
an ambiguous location, or a symbolic link below the pinned root is retained.
Recovery rechecks eligibility and treats an absent file as already removed.

OpenCode cleanup runs its configured binary as `session delete <session-id>`
with isolated configuration and the recorded data home. It checks session ID
metadata before and after the command. It retains sessions with untracked child
sessions because the native command also deletes descendants. Native database
deletion makes pages reusable. It does not promise to shrink the database file.

Enabling the switch starts a paced backlog pass over stored worker ownership.
Each batch examines at most 16 workers and 16 native ownership records. The
pass persists its cursor and deletion journal across service restarts. It uses
saved launch metadata, or a stored result and the recorded account mapping when
the host folder has already been removed. Missing ownership or home information
leaves a session untouched. Existing coordinator history is retained until Torii
has recorded enough confirmed replacements to identify the older generations.

The cleanup never removes account homes, memory, settings, authentication,
credentials, untracked or interactive sessions, or sessions used by active
workers. Muse storage, archived Codex sessions, and provider auxiliary files
are outside this switch.
This control is absent from MCP and Telegram menus.

### What is recorded

| Area | Codes |
| --- | --- |
| `service` | `started` (with the previous exit cause), `unclean-exit`, `restart-requested`, `restart-refused`, `restart-dropped`, `restart-backup-failed`, `loop-crashed`, `loop-stopped`, `loop-failed` |
| `coordinator` | `reader-failed`, `session-closed`, `steer-uncertain`, `steer-closed`, `steer-failed` |
| `hosts` | `log-delete-failed` (throttled; durable exponential retry back-off) |
| `worker` | the worker `failure_code` when a worker ends without success (`start_failed`, `execution_failed`, `auth_failed`, `host_died`, `pending_work`, `interrupted`, `owner_stopped`, `goal_unmet`, and others; `waiting_for_secret` is not a problem), `steer-refused`, `steer-uncertain`, `steer-closed`, `steer-unsupported`, `steer-not-arrived` (still unconfirmed when the worker ended), `goal-uncertain`, `goal-closed`, `goal-unsupported`, `opencode-permission-rejected`, `input-failed`, `stop-failed`, `cancel-failed`, `launch-cleanup-failed`, `ownership-cleanup`, `ownership-recovery`, `ownership-bootout` |
| `accounts` | `rotated`, `codex-rotated` (automatic Codex switching only), `unavailable`, `status-*` and `codex-status-*` (a changed usage-refresh error), `signin-*` |
| `shared-mcp` | `selection-invalid`, `source-missing`, `source-unreadable`, `source-invalid-json`, `entry-missing`, `entry-invalid`. Details name the affected entries, never their values. |
| `secrets` | `not-filled`, `wait-ended` (the asked Envelope expired or was cancelled), `vault-read-failed`, `vault-write-failed`, `vault-delete-failed`, `intake-failed`, `effect-failed` |
| `telegram` | `poll-<telegram code>`, `poll-stopped`, `reaction-timeout`, `reaction-failed`, `reaction-admin-required`, `reaction-admin-check-failed`, `callback-failed`, `command-menu-failed`, `bot-identity-failed` |
| `outbox` | `send-failed` (every attempt, with the attempt number), `edit-failed`, `image-unavailable` |
| `attachment` | `download-failed`, `larger-than-10-mib`, `larger-than-20-mb`, `not-a-supported-image`, `unsupported`, `unavailable` |

OpenCode stays off by default and requires `opencode-ai@1.19.3`. The real-binary
contracts verify ACP initialization, new and loaded sessions, prompts, cancellation,
usage, provider errors and retries, config isolation, and the guard hooks against
local model stubs. A chat session created with 1.19.0 resumes with 1.19.3 and then
with 1.19.0 again. Earlier messages, database schema and migration records survive
both transitions. Both versions open private records copies read-write, and
Torii's records and usage reads leave the source files unchanged. Before any future
version bump, repeat the round trip, read-write records-copy check and binary contracts.

[1.19.1 removes session sharing](https://github.com/anomalyco/opencode/releases/tag/v1.19.1),
which Torii already disables. [1.19.2 keeps normal upgrades on 1.x](https://github.com/anomalyco/opencode/releases/tag/v1.19.2).
[1.19.3 adds optional reasoning variants](https://github.com/anomalyco/opencode/releases/tag/v1.19.3),
which Torii does not configure. ACP, storage migrations, shell execution and the
`shell.env` and `tool.execute.after` hook implementations are unchanged across
these releases. The upstream shell output race remains a known risk. Its stream
reader still does not join through EOF before the exit scope closes. Five local
unguarded fast-shell attempts per version did not reproduce lost output. Keep
Torii's guard enabled.

For binaries in Torii's OpenCode folder, the owner's `/update` installs the target
commit's pinned OpenCode into a sibling version folder. It sets `opencode_binary`
to the installed binary before the service restarts. Binaries outside Torii's
OpenCode folder require a manual update on the host, as the `/update` card states.

At the first service start after a pin raise, an idle OpenCode chat reattaches to
its existing host and retires it. The chat then loads the same native session on
the new pin with `session/load`. A chat in the middle of a turn keeps its earlier
host until the turn finishes, then loads the same session on the new pin.

A running OpenCode worker reattaches to its earlier host and finishes the original
turn without a second prompt. A later steer of a finished worker starts a new host
on the process pin and loads the same saved session, account, and data root.
Fresh launches and reattachments without an accepted session still require the
exact pin. Reattachment still requires ACP protocol version 1.

The worker-local guard plugin sets `/bin/bash` as the shell. Its documented
`shell.env` hook supplies a private `BASH_ENV` startup file. Bash redirects
stdout and stderr into a mode-0600 file inside a mode-0700 launch folder before
it evaluates the original command. OpenCode still checks the original command
and working directory, and handles the process timeout and cancellation.
The `tool.execute.after` hook reads the file after command completion and
supplies the actual text to the model. It preserves exit status and timeout
or cancellation notices. A missing startup receipt or an unreadable capture
raises a tool error instead of reporting empty output.

The returned preview keeps the last 50 KiB and 2,000 lines, matching the pinned
defaults. Truncated results name a private full-output copy in OpenCode's
permitted `tool-output` folder, under its existing seven-day retention. Empty
files return `(no output)`. Nested Bash commands inherit the redirected output;
the startup hook unsets its capture variables before the command runs. Output
arrives at tool completion. The hooks do not change Claude or Codex workers.

Offline shell checks run real `/bin/bash` commands through the hooks with Node.
Both CI jobs install Node 22 so these checks cannot silently skip. To run the
opt-in binary checks, set `TORII_OPENCODE_CONTRACT_BINARY` to the installed
1.19.3 binary and run `python3 -m unittest tests.test_opencode_contract
tests.test_opencode_guard -v` with an isolated HOME and TMPDIR.

OpenCode worker failure codes describe whether a fresh spawn can help.

| Code | What happened | Can a fresh spawn help? |
| --- | --- | --- |
| `provider_stream_incomplete` | Three consecutive steps ended without a usable finish or tool progress. | Yes, if the upstream stream recovers. |
| `provider_guard_unavailable` | This launch has no valid guard registration proof. | Only after the guard loads correctly. |
| `provider_managed_config` | A managed plist exists, or its lookup failed. | Only after the managed-config conflict is resolved. |
| `provider_records_unavailable` | Final worker records could not be read. | Yes, if worker storage recovers. |
| `provider_error` | OpenCode rejected an ACP request or reported an upstream error. | Yes, for a transient upstream error. |
| `session_missing` | OpenCode could not load the requested native session (missing or unreadable). | Yes, as a new session. |
| `auth` | The key is unavailable, unreadable, or rejected with HTTP 401. | Only after account access is restored. |
| `out_of_credit` | The upstream returned HTTP 402. | Only after account credit is restored. |
| `rate_limited` | The upstream returned HTTP 429. | Yes, after capacity returns. |
| `max_tokens` | The answer reached the model output limit. | Only with a shorter task or a suitable output limit. |
| `account_changed` | The saved OpenCode account is no longer ready. | Yes, as a new run on the active account. A resume works again once the saved account is restored. |
| `session_busy` | Another host or native process owns the session. | Yes, with a new session. |
| `start_failed` | A launch prerequisite failed before the host started. | Only after the prerequisite is fixed. |
| `host_died` | The provider host stopped without an exit record. | Yes, if the host failure was transient. |
| `execution_failed` | Provider execution raised an exception. | Only after its cause is resolved. |
| `version_mismatch` | The binary did not report the pinned version. | Only with the pinned binary. |
| `invalid_session_id` | OpenCode returned an invalid native session ID. | Only after the provider fault is resolved. |
| `owner_stopped` | The owner stopped the worker. | Yes, if the owner wants a new run. |
| `cancelled` | OpenCode cancelled the prompt. | Yes, if the cancellation cause is gone. |
| `refusal` | The provider refused the prompt. | Only with an acceptable task. |
| `max_turn_requests` | OpenCode reported its turn request limit. | Yes, with a smaller task. |
| `invalid_stop_reason` | OpenCode returned an unsupported prompt stop reason. | Only after the provider fault is resolved. |
| `receipt_timeout` | An ACP request received no response. | Yes, if the provider recovers. |
| `disconnected` | The ACP connection closed before completion. | Yes, if the connection failure was transient. |

New Claude launches read a private `<state_dir>/shared-mcp-<sha256>.json` file.
Each file has mode 600 and holds only the valid shared MCP entries selected for that
launch. Torii refreshes a reused file's modification time. On a later valid
launch, it removes other versions after an hour without use. Missing or invalid
sources omit the file from the new launch. Do not copy these files into host
directories or reports.

A row has `id`, `created`, `area`, `code`, `detail`, and the ids it concerns:
`topic`, `task`, `worker`, `message` (a `messages` row), and `attachment`.
`detail` is at most 300 characters and may contain an owner-supplied restart
reason. Before a detail is saved or
logged, the service replaces a bot-token shape with `[token]`, URL credentials
with `[credentials]`, a URL query or fragment with `?[query]`, and an email
address with `[email]`. This filter does not remove every form of private
content. Do not put secrets in restart reasons or upload problem records to public issues.

Telegram polling, callback, command-menu, and bot-identity failures save at
most one row per code per minute. The next saved row starts with
`repeated=N` for the rows it skipped.

Each start records the previous run's exit: `restart reason=...`, `signal`,
`loop failed: ...`, or `error: ...`. A run that ended without that record, for
example after `SIGKILL` or a crash, gives `unclean-exit`. Each start also
deletes rows older than 30 days.

Each loop crash records `service/loop-crashed`. After six repeated errors, a
non-essential loop pauses and retries after 15 minutes. Repeated pauses double
the delay up to two hours. Ten minutes of healthy running resets its crash count
and retry delay. The first pause records `service/loop-stopped` with sanitized
error details and queues one home notice with a plain loop name. Later pauses
in that service run do not repeat the problem or notice. A database error while
saving the pause report does not stop the service or prevent the retry.
Essential loops, including restart handling and updates, retain their fatal
behavior after six errors.

### Log line format

```text
<time> WARNING coordinator.problems problem area=<area> code=<code> [topic=<id>] [task=<id>] [worker=<id>] [message=<id>] [attachment=<id>] [detail="<detail>"]
```

These codes log at `ERROR`: `reader-failed`, `session-closed`, `unclean-exit`,
`loop-crashed`, `loop-stopped`, `loop-failed`, `restart-backup-failed`, `poll-stopped`, and
`intake-failed`. Every other code logs at `WARNING`. Examples:

```text
2026-09-24T06:02:11.412 WARNING coordinator.problems problem area=worker code=start_failed topic=-1001234567890:68 task=41 worker=207 detail="FileExistsError: [Errno 17] File exists: worker-207 at providers.py:201"
2026-09-24T06:03:40.018 ERROR coordinator.problems problem area=coordinator code=session-closed detail="exit=1 failure=RuntimeError: Provider changed the coordinator session ID at session.py:241"
2026-09-24T06:05:02.905 WARNING coordinator.problems problem area=telegram code=poll-502 detail="repeated=4 retry_after=None failures=5"
```

### Audit

```sh
python3 -m coordinator ctl call problems.summary
python3 -m coordinator ctl call problems.summary since=2026-09-23T00:00
python3 -m coordinator ctl call problems.list
python3 -m coordinator ctl call problems.list area=worker limit=200
python3 -m coordinator ctl call problems.list since=2026-09-24T00:00 code=session-closed
grep ' problem ' ~/.local/state/telegram-agent-coordinator/logs/service.log
```

`problems.list` returns rows newest first. It takes `since` (an ISO time; local
time without an offset), `area`, `code`, and `limit` (1 to 500, default 50).
`problems.summary` counts rows by area and code since `since`, default the last
24 hours. Both are read-only and are also coordinator MCP tools. Times in the
output are local ISO times, like the log file.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| No reply | Check `status`, the project topic, outbox rows, `problems.summary`, and service logs. |
| Pairing fails | Generate a new code and send it within 30 minutes as your own group-topic user. |
| Ordinary messages do not arrive | Check bot administration rights, topic placement, and the single poller. |
| A worker does not start | Check the task worktree, native CLI, selected account, and session lock. |
| A message is uncertain after restart | Read the coordinator restart event and inspect prior effects before continuing. |
| A session is already owned | Settle its existing writer. Preserve its native history. |

The offline suite uses fake Telegram and provider processes. It does not verify
the running service. See [Verification](verification.md) for that boundary.

## Codex goal tools

Codex workers start with goal tools only when they have a goal. A goal
added during a turn takes effect after that turn and a restart of the worker's
app-server on the same native thread. Torii holds steers until the goal is
acknowledged. Existing hosts retain their launch options on reattachment.

## Updates from Telegram

Updates from Telegram default to off. To enable them, open **/accounts → Models
& Codex** and tap **Turn Telegram updates on**. `/update` then appears in the
owner's command menu. The command shows Torii changes and available agent CLI
versions before the owner taps **Update**. Running chats and workers keep going.

The [update and recovery protocol](release.md#updates-from-telegram) defines the
checkout safety checks, CLI prefix discovery, restart health signal, and rollback.
Use the same settings button to turn updates off. The local control is
`python3 -m coordinator ctl call service.updates enabled=false`.

### Report folders outside active ownership

`cleanup.stray_report` controls a report-only storage scan. It is off by default.
Enable it with `python3 -m coordinator ctl call cleanup.stray_report enabled=true`.
Use the deployment's usual `--state-dir` when it is not the default state folder.
Enablement schedules an initial scan. Later scans run at 03:00 in the service's
local time. `run=true` requests another scan while the switch is on.
`enabled=false` stops future scans. Calling the op without parameters returns
the switch, configuration and latest report.

The scanner lists direct child folders of `state_root`, which defaults to the
parent of Torii's service state directory. It also lists unowned direct child
folders of the canonical job results directory when their allocated size is
larger than `results_min_bytes`, which defaults to 1 GiB. The service directory
is protected as a whole in the first pass. Its results receive a separate pass.

Open, waiting and interrupted jobs protect their canonical copy, scratch and
results paths. A task or job id in a folder name also protects the folder while
that job is active. Configured chat folders, unfinished worker folders and
undelivered output paths protect overlapping candidates. An unfinished worker
also protects its job's results even after the job closes. The reporter reuses
the path constructors used by job save and worker storage cleanup.

LaunchAgent and LaunchDaemon path fields and running process file paths protect
folders that contain those references. The process probe uses `lsof` path output
without process arguments or environments. The LaunchAgent probe reads plist
metadata and extracts only the working directory, program, absolute arguments,
stdout and stderr paths. It does not retain other plist fields. Paths from a
shared ancestor outside a candidate do not imply ownership of all its children.
`service_paths` adds explicit service-owned path references.
Default launch folders include both the service HOME and OS account home, plus
the system LaunchAgents and LaunchDaemons folders. `launch_agents` overrides them.

The scan opens directories without following symlinks. It reads only filesystem
metadata from candidate folders, never their file contents. It excludes agent
home folder names at every depth. It never descends into Torii's service-private
folders through the state pass. It does not delete, move or change candidates.

Each report has paths, allocated bytes, newest regular-file mtime, a guessed
task id and known chat name, totals by category, and changes since the previous
complete report. Allocated bytes use `st_blocks * 512`. Hard links inside one
candidate count once. Shared APFS extents and hard links between candidates can
still overlap, so totals are not a measurement of uniquely reclaimable space.
Excluded agent homes contribute no bytes. The report records their count.

The scanner stops after 5,000,000 metadata entries, 1,800 seconds or depth 64.
Failed reference probes, unreadable folders, filesystem crossings and these limits
make the report incomplete. Such reports do not advance the baseline or queue
a summary. The non-essential service loop can pause independently after errors.

Complete reports queue one short home-chat summary only when their total grew
by more than `growth_bytes`, which defaults to 2 GiB. The first complete scan
compares against zero. Equal or smaller growth queues nothing. Delivery uses
the normal durable outbox. The scan makes no Telegram API calls.

The Store retains the last `keep` reports, which defaults to 7 and accepts 1
through 31. `config` accepts a JSON object with any of `state_root`,
`launch_agents`, `service_paths`, `results_min_bytes`, `growth_bytes` and `keep`.
Paths must be absolute and outside agent homes. Both path lists are configurable.
For example, `config='{"results_min_bytes":536870912,"growth_bytes":2147483648,"keep":7}'`
sets the result limit to 512 MiB and the growth threshold to 2 GiB.
