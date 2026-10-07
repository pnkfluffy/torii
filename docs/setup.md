# Setup reference

This page holds the setup detail behind the short steps in the
[README](../README.md). Day-to-day controls are in [Operations](operations.md).
This release supports macOS. Other platforms are unsupported.

## Setup

Run `python3 scripts/setup.py` in your own terminal. It checks the computer,
opens [BotFather's in-app bot creator](https://t.me/BotFather?startapp) on an
interactive Mac, and prints a QR and the link for your phone. Tap **Create a
New Bot**, choose a name and a username ending in `bot`, then tap **Copy** next
to the token. For older clients, send `/newbot` to @BotFather.

Setup can pick up a new copy from this Mac or your iPhone through Universal
Clipboard. macOS may ask to allow the paste. If pickup is unavailable, paste
the token in the terminal and press Return. Input stays hidden. Setup never
reads old content when pickup starts and leaves the clipboard unchanged.
Pickup stops after five minutes; hidden paste stays available. A rejected
token returns to this screen. A connection failure lets you retry the check.
Setup checks the bot and webhook. Make a bot just for Torii.
A bot another program polls will stop working
in that program. Setup checks existing commands and description before using a
bot it has not used. A webhook must be removed deliberately before polling.
No token enters an agent chat, argument, or environment variable.

On an interactive Mac, setup next connects Claude in your browser. Click
**Authorize**, or paste a code in the terminal if Claude asks for one.
Press Control-C to skip, or use `--skip-claude` to connect Claude or ChatGPT in Telegram later.
Setup registers the profile once `claude auth status` reports the login.
Torii never reads the saved login itself. Claude Code uses it directly.

Setup then installs the background service and opens a single-use group link.
It prints another QR and the link for your phone. Narrow terminals or a QR
generation failure show the link alone.
In Telegram, pick a group **you own** and tap **Add as admin**. The link suggests
**Manage topics**, **Delete messages**, and **Pin messages**. Keep those rights.
The code expires after 30 minutes. Torii checks that the sender is a real person
and the group's creator. Usernames are display text. An anonymous admin must
turn off **Remain anonymous** and retry. Torii deletes the pairing message after
it accepts the code. Setup waits on local state and never polls bot updates.

No group yet? In Telegram tap **New Group**, give it a name, and create it.
Then return to the terminal and press Enter. The group picker may vary by client;
setup works with a group created before you open the link.

If Topics are off, Torii posts these instructions in the group:

> Paired. One step left: turn on Topics so each project gets its own thread.
> Tap the group name → Edit → Topics → turn on → Save.

Tap **Check again**. Only the owner can use this button. Turning Topics on can
upgrade a basic group to a supergroup; Torii follows the new group ID. The bot
cannot turn Topics on itself. Check that it still has **Manage topics** after
that upgrade. Torii also checks its rights before creating a topic.

Torii creates a **Torii** topic. If Claude or ChatGPT is already connected, send your first
request there. If you skipped terminal sign-in, tap **Connect Claude** or **Add ChatGPT**.
For Claude, the bot starts sign-in to a fresh Torii profile. Authorize in your browser, then
paste the code in the topic of the sign-in card. Torii deletes it at once.
For ChatGPT, follow the device-code card in your browser.
The DM is only for secrets. Send work in group topics.
Torii does not adopt your personal Claude home or copy its history.
Claude Code may ask macOS once for access to its own login in a new profile.
**Add ChatGPT** connects a ChatGPT account. Either Claude Code
or Codex must be installed. Connecting ChatGPT alone releases saved work and runs
the main chat on the active account. Its accounts never switch automatically for the main chat.

Fresh setup holds work with `execution=pairing`. A connected Claude or ChatGPT account
changes it to `agents`. Existing group installs with no execution key continue
to run agents. `--pair-only` is an explicit operator choice; the default installer
has no such flag. Rerun `./scripts/install-service.py` to remove it.
`--enable-agents` remains accepted for compatibility.

Send your first request in **Torii**. Torii holds it and asks for a project
name. Tap the suggestion or **Type a name**, then send a name on one line with
at most 40 characters. Other messages add to the held request. Rejected names
stay out of the request. The request survives a restart. Torii creates a Git
project under `~/Projects/torii`, or the folder set with **Projects folder** in
`/projects`, creates its group topic, and releases the held request there.
Existing folders need `/setup use PATH`. Use **New project** in `/projects`
for another project and topic.
Each project keeps its own conversation.

Send `/setup` in **Torii** to show setup status. During sign-in it reposts the
same card. Use **Fill privately** for secrets. Credential values reach
Telegram before Torii deletes them; Telegram retains its own records.
A deleted project topic stops accepting work. Torii posts a notice in **Torii**;
use **New project** in `/projects` to create a replacement. If **Torii** is
deleted, setup recreates it. Saved project folders and native history remain on
this Mac.

`python3 scripts/setup.py --status` reads setup state without credential values.
A bot removed from the group shows recovery instructions and a link. Add it
back to the same group as the owner to resume. If the owner leaves, Torii pauses
new work while current sessions and result delivery continue. Rejoining resumes.
A Telegram group ownership transfer changes status text but does not change
Torii's paired owner. A bot added to another group refuses it and leaves.

To move Torii to another group, run `python3 -m coordinator pair --replace`.
Old topics are disabled; projects, accounts, and native sessions remain.
An install that used private chat refuses to poll after upgrade. Status prints
recovery instructions. `pair --replace` clears its old routing. Rerunning setup
also offers an explicit terminal confirmation to move it to a group.
Torii archives old DM topic records locally so their task and session history
remains available. It does not map those topics into the new group.

## Models and effort

The coordinator defaults to `claude-opus-5-5`. `--coordinator-model` selects
another model at service startup. In `/accounts`, open **Models & Codex ›**,
then **Coordinator model** to set it from Telegram.
Worker model settings are separate. Codex workers default to `gpt-6.1-sol`
unless the owner sets another Codex model.

Parent coordinators launch Claude with `--effort max`. Workers default to
medium effort. The coordinator can pass `effort=low`, `medium`, `high`, or
`max` to `workers.spawn`. Claude workers receive `--effort`. Codex workers
receive `-c model_reasoning_effort=<level>`, with max mapped to xhigh.
Ultracode is off for Claude coordinators and Claude workers. These settings
apply to new launches and resumes, not to native processes that are already
attached. Native subagents inherit session effort unless their definition
overrides it; use medium for delegated children, including children of a
coordinator.

## Agent usage policy

Torii creates `USAGE.md` in its state directory from
[`coordinator/defaults/USAGE.md`](../coordinator/defaults/USAGE.md) when the
file is missing. Edit the state copy to change agent guidance. Torii keeps your
edits when it starts again. New parent sessions and workers read the current
file. An existing parent reads edits with its next message. No service restart
is needed for a policy edit. In Telegram, open `/accounts`, **Models & Codex ›**,
then **Usage policy** to read or edit the policy.

## Projects, home channel, and task worktrees

Open `/projects` in Telegram. **New project** makes a project folder and topic.
**Projects folder** sets where new projects go. In an unlinked project topic,
**Link this topic** opens the link flow. In a linked topic, **This topic ›** shows
its folder, **Change folder**, and **Open jobs**. A folder change requires no
open job. Linking or changing a folder does not move files or start a model.

`/setup` and `/start` remain hidden commands for pairing and linking.
`/project new NAME` and `/project folder PATH` remain hidden aliases. Use
`/project new NAME` in General, where Torii cannot show project input prompts.

The first enabled, linked channel becomes home, stored as
`coordinator_home_topic`. Use the owner control `topic.home` with a linked,
enabled topic to change home. Pass `clear=true` and the current home topic to
clear home, even if it is disabled. Existing native conversations keep their
session IDs when home changes. A running parent keeps its launch-time
registered-channel view until its next native launch; changing home does not
restart it.

The coordinator creates one worktree per committed task. The branch is
`torii/task-<id>` and the folder is `worktrees/task-<id>` under the state
directory, where `<id>` is the global task ID. Topics that share a repository
therefore never share a branch. A task created before this rule keeps its saved
worktree and branch. Questions and brainstorming do not create tasks.

Each topic has its own thread and its own task list, even when topics share a
folder. The coordinator sees the topic ID on every message, and a task belongs
to the topic whose message asked for it. Topic names are unique in a chat:
setup and rename refuse a name that another topic uses.

## Account readiness check

Account checks run automatically after sign-in. For an explicit terminal check, use `python3 -m coordinator accounts check`,
while you are at the Mac. It checks enabled accounts one at a time and allows up
to three minutes per native step for sign-in or Claude Code's own Keychain approval. macOS may ask
once per credential; Torii cannot combine those OS dialogs into one approval.
The command reports account readiness without printing credentials. It does not
perform login for you.

The service then checks usage for every enabled account at startup, every five
minutes, and after a limit. A failed check is retried after 30 seconds, then at
doubling gaps up to five minutes. An account whose last check failed, or whose
last good check is more than ten minutes old, shows `stale since` that time.
Claude Code can still ask macOS for access to its own login if an account has not
been approved.

## Starting the service

`python3 scripts/setup.py` installs the service for group setup. Manual
installs can use `./scripts/install-service.py` for the macOS
LaunchAgent; `./scripts/run-service.sh` runs the same service in the
foreground. Use only one service poller for the bot. Add
`--coordinator-model YOUR_MODEL_ID` to either command if the default model is
unavailable. Each agent-enabled service start queues an online notice in every
enabled topic. After a relaunch, the service resumes each topic's exact saved
coordinator session.

## Messages and reports

The service saves each owner message, then steers it into the coordinator. It
delivers messages individually and in order, including messages sent while a
turn runs. The coordinator reports progress and completion separately; see
[Message reactions and job reports](operations.md#message-reactions-and-job-reports)
for what each reaction means. A report names the
feature and its state: what changed, what works, what is blocked and by what,
and the one thing you must do. Task numbers, PR numbers, commits, run IDs,
versions, and proof paths stay in task notes and worker reports.

Send JPEG, PNG, WebP, or GIF images, or files of any type, with instructions in
a caption. The limit is 10 MiB per image and 20 MB per file, the most Telegram
lets a bot download. Torii stores images and files privately and gives the
coordinator their paths. It can include a short quote when you reply to an
earlier message. See [Images, files, and replies](operations.md#images-files-and-replies).

Voice notes are transcribed locally on the Mac. Transcription sets itself up
on the first voice note, with a one-time download of a couple of GB.

## Shared MCP servers

Shared MCP servers are opt-in for Claude parents and workers. Add server names
from the Torii home `~/.claude.json` to the checkout's gitignored
`local-overrides.json`:

```json
{"shared_mcp_servers": ["example-mcp"]}
```

The default is none. Preserve other keys in that file. Remove only
`account_switch_thresholds` to clear account reserves. Pinned releases read
this file from the release checkout, not from `TORII_HOME`. See
[Architecture](architecture.md) for how Torii validates and passes the entries.

## Advanced manual pairing

`./scripts/setup-telegram.py` saves the bot token outside Git with private
permissions. `python3 -m coordinator bot` checks bot identity without printing
the token. Do not put the token in a command or chat message.

`python3 -m coordinator pair` prints a group link when the bot username is known,
or a `/pair CODE` command otherwise. `--group` is a hidden compatibility alias.
A link has this form:

```text
https://t.me/<bot>?startgroup=<code>&admin=manage_topics+delete_messages+pin_messages
```

A `/pair CODE` command works in General, a basic group, or a project topic.
Send it as the group creator within 30 minutes. General is never bound as a
project. Pairing inside a topic keeps its project setup guide.
The bot must be running to receive pairing. Use `./scripts/run-service.sh`
or install the service. `--pair-only` polls and delivers without agents.
Use exactly one poller per bot.

Telegram documents [group bot links](https://core.telegram.org/api/links#group-channel-bot-links),
[deep-linking](https://core.telegram.org/bots/features#deep-linking),
[administrator rights](https://core.telegram.org/bots/api#chatadministratorrights),
and [forum owner requirements](https://core.telegram.org/api/forum).
The picker, `/start` sender and location, and admin rights after upgrade need
[a live check on a separate Mac](group-setup-live-test.md).
