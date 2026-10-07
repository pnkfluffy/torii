# Operations

Run commands from this checkout. Use one service poller per bot. The foreground
service and the macOS LaunchAgent are alternatives.

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
and native sessions stay on this Mac. Missing Manage topics shows **Fix permissions**.

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
./scripts/install-service.py --enable-agents --coordinator-model YOUR_MODEL_ID
```

Omit `--coordinator-model` to use `claude-opus-5-5`. Keep the checkout and
Python executable at the paths used during installation. Reinstall after moving
them. A sleeping Mac does not process messages. The LaunchAgent starts at login.

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

At upgrade, confirmed legacy 👀 reactions remain desired and sent. Pending
legacy acknowledgements are cancelled. Disabled rows stay disabled. The
migration makes no Telegram calls, so no existing reaction changes at upgrade.
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
The code never enters the vault, the log, the state database, or a reply. Other
text in that topic gets a pointer back to the card and is not passed on. The code
works once and only with the login that showed the link. A code sent to the bot
DM is deleted, and the reply points back to the card.
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
Torii never reads or passes Claude sign-in tokens. New work picks the eligible
account with the soonest weekly reset, with alias as the tie-break. A running
parent stays on its account until it is ineligible. The causes are 95% usage
in a 5-hour, all-model weekly or Fable weekly meter, a local reserve, a hold,
a signed-out profile, or an account the owner disabled. The parent waits for
its turn and background tasks to finish, then restarts under the next eligible
account and resumes the exact native transcript without copying history.
A worker switches accounts when Claude rejects it for usage.
A usage rejection mid-turn restarts the parent at once and asks it to check
the interrupted messages. Background tasks stopped by that restart have lost
results. A callback names them and asks the parent to check their state.
Stale snapshots remain eligible unless a meter or hold blocks them. If every
account is full, work waits for the earliest reset and Torii sends one notice.

When a Claude account is limited or at least 95% used, `/accounts` shows
**Claude reset ↗**. It opens [Claude's usage page](https://claude.ai/settings/usage).
Use that page to spend a Claude reset. Torii cannot spend one or see its banked
reset count. Check which Claude account is signed in to the browser.

In `/accounts`, **Add ChatGPT** starts a Codex device-code
sign-in in a new home `~/.codex-accounts/.torii-HEX`. The card shows a link and a
one-time code; enter the code on the ChatGPT page within 15 minutes. The same
**Cancel sign-in** and **Try again** rules apply.

Each parent launch prefers an available Claude account, then the active ChatGPT
account. A running parent keeps its provider. Switching provider starts a fresh
native conversation with a continuity notice and a note in the home topic.
The main chat returns to Claude at the next launch after Claude recovers,
including after its one-hour idle wind-down. ChatGPT parent accounts never
switch automatically, even with worker auto-switch enabled. At the active
account's limit, the main chat saves messages and pauses until the owner picks
another account with `/accounts` or the limit resets. `/health` names the parent
provider or shows its paused state. `/tldr` uses an ephemeral read-only ChatGPT
thread when Claude is unavailable. Saved secrets and worker goals work with both providers.

The ChatGPT parent exposes Torii MCP tools directly with
`mcp_servers.torii.omit_tools_from=["deferred","code_mode"]` and
`default_tools_approval_mode="approve"`, under `approvalPolicy="never"` and
`sandbox="danger-full-access"`. These are launch overrides, not account config
edits. On Codex 0.159.2, the default model sends tool schemas in Responses Lite
`input` items of type `additional_tools`. The opt-in contract test checks the
advertised tool names and an actual write to an isolated outbox.

Automatic ChatGPT switching for Codex workers is off by default. Codex work then
runs on the account you last picked. With no pick, Torii uses the signed-in
default home, else the first signed-in account in alias order. Adding an account
keeps a signed-in account in use. Tap **Use … for ChatGPT** in `/accounts`, or
ask the coordinator to switch. These buttons appear only when auto-switch is off.
At 80% used (20% left) in a 5-hour or weekly window, or when Codex rejects a turn at its usage
limit, Torii posts one notice per account and window with the usage, the reset
time, the other accounts' headroom, and the banked reset count. After a rejection, Codex workers wait as
`waiting_for_quota` and resume on the active account after a switch or the reset.
`/accounts` shows **Turn on ChatGPT auto-switch** or
**Turn off ChatGPT auto-switch**. With it on, Codex worker accounts
switch at 95% or on a usage-limit rejection, a Codex worker fails with
`accounts_unavailable` when every Codex account is limited, and the notice comes
when the last usable account reaches 80% used.

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
ChatGPT has 5-hour and week windows. A `next` tag marks the next Claude account,
and `active` marks the ChatGPT account in use. Limited rows say `back in X`.
Stale rows say `stale since` with a time. Local per-account reserves appear as
`reserve N%` and have no setting button. Signed-in rows need no separate card.

**Models & Codex ›** opens **Worker model**, **Codex model**, and
**Coordinator model**. It also has **Turn Codex off** or **Turn Codex on**, and
**Usage policy**. **Opus 5.5** (`claude-opus-5-5`) is the preset for both Claude
roles. The service reads the `coordinator_model` setting each time it launches the
coordinator host and uses the `--coordinator-model` flag when the setting is
clear. Worker and Codex model changes apply to future dispatches. Coordinator
model changes apply at its next launch.

`/projects` lists project folders and their linked topics. **New project** asks
for a name as a reply to its card, then makes a Git folder and opens a topic.
**Projects folder** sets the folder for future projects. In a linked topic,
**This topic ›** shows its folder and has **Change folder** and **Open jobs**.
An unlinked project topic has **Link this topic** instead. The Torii control
topic has neither topic button.
The coordinator creates a worktree when it commits to a task. A task may have
several agents in its worktree. The parent coordinates shared edits.

Typed commands send a new card. Taps and input replies edit that card in place.
Buttons that ask for text, such as a model ID, a project name, a folder path, or
the usage policy, take only a Telegram reply to their card. A plain message in
the topic goes to the agent as usual.
**Back** returns to the parent card. The accounts, projects, and secrets lists
have no Back button. Old buttons expire when another card opens in that topic.

## Credentials

A task that needs a credential asks for it by name with `secret.ask`. The
service posts an Envelope card in the topic. To give the value:

1. Tap **Fill privately** on the card within 10 minutes.
2. In the private chat with the bot, send the value as one message. Use one line
   of 16 to 4096 printable characters that does not start with `/` and is not
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

An expired, cancelled, or revoked request has **Ask again**. It reopens the
request with the same reason, consumer, and job. If a previous value remains
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
The owner accepts this residual for both providers. Torii applies its scrubber
to captured results, events, journals, logs, and Telegram output. The event
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
plist change.

This release supports macOS. The vault uses the file backend, POSIX permissions,
atomic writes, and the Python standard library. The service uses a launchd
LaunchAgent installed by `scripts/install-service.py`. Use macOS FileVault for
disk protection at rest. For stronger isolation, run the service as its own OS
user. Cross-platform implementation paths are unsupported in this release.

## Images, files, and replies

Send a JPEG, PNG, WebP, or GIF photo or document with instructions in its caption.
The limit is 10 MiB per image. The service saves images under
`images/message-ID/` in the private state directory. If an image cannot be
saved, the message text still reaches the coordinator, with a note that the
image could not be downloaded. The owner gets one reply that says the text went
through without the image and asks for the image only. Each album message keeps
its own images, so one failed image does not drop the others.

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

### What is recorded

| Area | Codes |
| --- | --- |
| `service` | `started` (with the previous exit cause), `unclean-exit`, `restart-requested`, `restart-refused`, `restart-dropped`, `restart-backup-failed`, `loop-crashed`, `loop-failed` |
| `coordinator` | `reader-failed`, `session-closed`, `steer-uncertain`, `steer-closed`, `steer-failed` |
| `worker` | the worker `failure_code` when a worker ends without success (`start_failed`, `execution_failed`, `auth_failed`, `host_died`, `pending_work`, `interrupted`, `owner_stopped`, and others; `needs_input` and `waiting_for_secret` are not problems), `steer-uncertain`, `steer-closed`, `steer-unsupported`, `steer-not-arrived` (still unconfirmed when the worker ended), `goal-uncertain`, `input-failed`, `stop-failed` |
| `accounts` | `rotated`, `codex-rotated` (automatic Codex switching only), `unavailable`, `status-*` and `codex-status-*` (a changed usage-refresh error), `signin-*` |
| `shared-mcp` | `selection-invalid`, `source-missing`, `source-unreadable`, `source-invalid-json`, `entry-missing`, `entry-invalid`. Details name the affected entries, never their values. |
| `secrets` | `not-filled`, `wait-ended` (the asked Envelope expired or was cancelled), `other-task`, `vault-read-failed`, `vault-write-failed`, `vault-delete-failed`, `intake-failed`, `effect-failed` |
| `telegram` | `poll-<telegram code>`, `poll-stopped`, `reaction-timeout`, `reaction-failed`, `reaction-admin-required`, `reaction-admin-check-failed`, `callback-failed`, `command-menu-failed`, `bot-identity-failed` |
| `outbox` | `send-failed` (every attempt, with the attempt number), `edit-failed`, `image-unavailable` |
| `attachment` | `download-failed`, `larger-than-10-mib`, `larger-than-20-mb`, `not-a-supported-image`, `unsupported`, `unavailable` |

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

### Log line format

```text
<time> WARNING coordinator.problems problem area=<area> code=<code> [topic=<id>] [task=<id>] [worker=<id>] [message=<id>] [attachment=<id>] [detail="<detail>"]
```

These codes log at `ERROR`: `reader-failed`, `session-closed`, `unclean-exit`,
`loop-crashed`, `loop-failed`, `restart-backup-failed`, `poll-stopped`, and
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
