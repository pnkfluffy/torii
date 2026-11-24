# Torii

Torii runs local agents for one Telegram owner. Each project topic has a
persistent coordinator and its own work. The coordinator can answer, start
workers, check their results, and report back in that topic.

Torii is independent of Anthropic, OpenAI, and Telegram. The core service uses
Python's standard library. The source is shared as-is; outside pull requests are
not accepted. Send feedback through issues. Report vulnerabilities through
GitHub private vulnerability reporting, without credentials or transcripts.

## Install Torii

You need macOS 13 or newer, or Linux with a systemd user session, Git, Python
3.9 or newer, and a supported provider CLI. Install
[Claude Code](https://code.claude.com/docs/en/setup) or the
[Codex CLI](https://developers.openai.com/codex/cli), and use an account with
access to the selected model. The machine must stay awake and online. Voice
transcription is optional, macOS-only, and requires macOS 14 or newer on Apple
silicon. It downloads a separate environment and model on first use.

Create a dedicated bot with `/newbot` in [BotFather](https://t.me/BotFather).
Use the repository URL supplied for your installation:

```sh
git clone https://github.com/example/torii.git ~/torii
cd ~/torii
python3 scripts/setup.py
```

Run setup in your own terminal. Enter the bot token only at its hidden prompt.
Never put the token in a command, agent chat, or issue. If it appears in a chat,
revoke it in BotFather and use the replacement at the terminal prompt. The
installer saves the path of the Python interpreter that runs it. Keep that
interpreter installed. Use its absolute path instead of `python3` if needed.

In Telegram, select a group you own and add the bot as an administrator. Keep
**Manage topics**, **Delete messages**, and **Pin messages** enabled. Turn on
**Topics** if asked, then tap **Check again**. In the **Torii** topic, select
**Connect Claude** or **Add ChatGPT** and follow the sign-in card. Send the
first request and choose its project name. Torii holds work until a provider is
connected. Use **New project** in `/projects` for another project.

### Agent-assisted setup

Follow this section only when the owner asks you to install Torii. Never ask
for, read, print, or store the bot token, account credential files, or sign-in
codes. Do not sign in for the owner or run nested agents. Preserve native
account homes and history. Check Git, Python, and the selected provider CLI
with their `--version` commands. A version check does not prove sign-in or
native protocol support.

Use the repository URL the owner supplied. Use `~/torii` unless the owner names
another folder. If the destination is an existing Torii checkout, use
`git pull --ff-only`. If it contains something else, choose a separate folder.
Do not replace it. Guide the owner through BotFather without receiving the
resulting token.

On a Mac with a GUI session, open the setup launcher in the owner's Terminal:

```sh
open -a Terminal ~/torii/scripts/setup.command
```

Use the actual clone path. Do not attach to or inspect that Terminal window.
On Linux, over SSH, or if opening Terminal fails, give the owner the setup
command above to run in their own terminal. Do not run `scripts/setup.py`
through an agent terminal or pseudo-terminal. After the owner says setup is
complete, run `python3 scripts/setup.py --status` with the selected interpreter.
Report its output and leave further interaction to the owner.

### Linux

Run setup as a normal user with Linux-native provider sign-ins. Do not copy
credentials, service state, or native history from another machine. The
installer uses a systemd user unit. `scripts/install-service.py --write-only`
writes the unit without changing systemd or linger. A manual install can use
`python3 scripts/install-service.py`; it enables the user unit and linger.
If local authorization blocks linger, an administrator must enable it for the
user. Setup uses `xdg-open` only when a display is present.

## Daily use

| Action | Control |
| --- | --- |
| Give or steer work | Send a message in an enabled project topic. |
| Answer a question | Reply to the coordinator's message. |
| Set a long goal | `/goal CONDITION` |
| Create or manage projects | `/projects` |
| Add or remove accounts, set models | `/accounts` |
| Manage saved credentials | `/secrets` |
| See system load and agents | `/health` |
| Catch up on a topic | `/tldr` |
| Check the connection | `/ping` |
| List commands | `/help` |

Images and files can carry instructions in a caption. Telegram limits each
bot download to 20 MB. The bot's private chat handles credentials and sign-in
codes, not work requests. Owner settings and usage policy determine provider,
model, effort, delegation, review, and shipping choices.

On the machine, list and inspect controls with:

```sh
python3 -m coordinator --help
python3 -m coordinator ctl list
python3 -m coordinator ctl describe tasks.create
python3 -m coordinator ctl call tasks.list
```

`ctl call` accepts `key=value` arguments. Use `--topic` with a topic ID or a
unique project name when needed. The CLI returns JSON. Exit codes 0, 1, 2,
and 3 mean done or queued, handler failure, usage error, and refusal.
`pending.signal` is local CLI only, refused over MCP and from worker sources.
Telegram sources are also refused. Any process with local CLI access can call
it.

The loadout supplies model and effort defaults for coordinators and workers.
`loadout.show` shows the effective defaults. Coordinators can use
`loadout.swap` and `loadout.unswap` for owner-requested temporary changes.
Swap creation requires the owner message ID and an end time within 30 days.
Coordinators can create, end or show swaps only for their own chat. Only the
home chat can create or end all-chat swaps by omitting `topic` on creation.
`loadout.show` without `topic` uses the calling chat. Values have no control
characters and appear quoted in prompts. Reasons appear only in
`loadout.show`. Set worker effort with the local CLI `effort.worker` control.
Codex service tier stays in the shared Codex configuration.

Idle job trimming is off by default. To enable it after 24 hours, run
`python3 -m coordinator ctl call cleanup.idle_trim enabled=true`. Add
`hours=N` to choose another interval. Set `enabled=false` to turn it off.
Jobs with armed waits are not trimmed.

The sweep removes dependency and cache folders in job scratch, and ignored
dependency, build and cache folders in work copies. Other scratch files, the
job branch and results stay. A later worker receives a
reminder to reinstall dependencies before tests or builds.

Finished agent session cleanup is off by default. Use
`python3 -m coordinator ctl call cleanup.sessions enabled=true` to move
eligible native transcripts older than 14 days to the Trash once a day. Set
`enabled=false` to stop the sweep. Open jobs, sessions referenced by their
handoffs, and each chat's two most recent native sessions stay. OpenCode stores
sessions together in a database. Its database moves only when all sessions in
it belong to the finished work and the database can move as one file.
OpenCode worker home cleanup runs independently. After its three-day grace
period, it saves usage and moves eligible homes from cleared jobs to the Trash.

When close-save is on, an open job with no queued, running or waiting worker
for 48 hours is parked like a waiting job: Torii saves its work and proofs,
then clears its worktree and scratch. Before the scratch clear, each Git tree in
scratch gets a bundle of the commits and stashes no surviving ref holds and a patch of its
changes and untracked files in `rounds/K/scratch-git/`, and the job notes name
the folder. Commits that only a reflog holds are pinned under
`refs/torii/scratch-reflog/` first. A `.git` file counts as a Git tree only when it points at a Git directory.
If one tree cannot be saved, the whole scratch stays, the partial save is removed and the park is held
with a cleanup problem that names the tree. A later save links the bundle of an earlier save of the same commits. A save that no
round records moves to a `torii-trim-task-*` folder in the Trash only when each of its files is also in a save
that stays: the same bytes, or a bundle of the same heads, left-out commits and object count. The job stays open, and the next worker
rebuilds the worktree. A park whose save fails is retried after 1, 2, 4 hours
and so on, at most a day apart; the hourly park check never parks a job again in the same round,
whatever its copy state, and a paused rebuild keeps its hold. When a saved `branch.bundle` is
gone, Torii rebuilds it from the commits the repository still holds or can
fetch from its remotes and the project's other clone. A saved ref that moved
stops the rebuild. If every source answers that the commits are gone, Torii
records one `saved-work-lost` problem and stops retrying; if a source gives no
answer, it records `saved-work-unverified` and keeps retrying. Copies of git-ignored files in `rounds/` are deleted 7
days after a done or dropped job closes. A `rebuildable` close rule
(`cleanup.close_rules rebuildable='[{"project": ..., "paths": [...],
"manifest": ..., "prefix": ..., "ref": ..., "command": ...}]'`) skips
ignored files whose sha256 matches the published manifest at `ref`, because
`command` fetches them again. Files that differ are saved as usual.

For an open job without a queued, running or waiting worker, the hourly sweep
moves scratch entries untouched for 48 hours to a `torii-trim-task-*` folder
in the Trash. It also trims scratch over 10 GiB and results over 2 GiB oldest
first, starting with entries at least 12 hours old. Records, reports,
`HANDOFF.md`, KEEP, `rounds/`, Git bundles, Git trees, referenced paths and files
that a process has open stay. A note, a prompt, a script or a `.md` file in the
job's results that names the job folder keeps every entry in it. The caps count
only entries that the sweep can move. While a job has such a worker, Torii
removes from its scratch only clean, merged Git worktrees. A path that the
reference scan may not read is skipped and recorded as a
`reference-scan-unreadable` cleanup problem.
When free space on the state volume is below 10 percent, idle jobs park after
2 hours and Torii records a `low-disk` cleanup problem. Torii deletes its
`torii-trim-task-*` and `torii-orphan-*` Trash entries after one day in the Trash.

When a worker of a job ends, Torii removes extra Git worktrees in that
job's scratch only if each is clean, has no unknown ignored files, no lock, no
live process, no KEEP link or proof, and its HEAD is on the remote main branch.
While another worker of the job is unfinished, only a worktree that nothing
wrote to for 48 hours goes.
Ignored dependency, build and cache folders go with it. The branch stays. The
managed job worktree is never removed this way.

Channel continuity includes UTC dates for recent messages and reply status
for owner messages. A reply not linked status does not prove that a message was unanswered.

To message another Torii chat or agent, follow "Messaging another chat or
agent" in [AGENTS.md](AGENTS.md). The recipient treats the event as a note, not
an owner instruction.

## How Torii works

The service accepts work only from the paired numeric Telegram owner in the
paired group. Names and usernames are display text. Other users, bots, edited
messages, and anonymous administrators cannot submit work. Forwarded messages
from others are content, not owner instructions. New topics need a project
binding before they accept work.

The service saves an incoming update and its cursor before acknowledging it.
It feeds saved messages to the topic coordinator in order, including during an
active turn. A message does not itself create a task. The coordinator creates a
task for committed work, delegates, and verifies results. Tasks use worktrees.
The service persists worker results and an outbox for replies. Retrying a
Telegram delivery never repeats agent execution.

A new job worktree without a `base` starts from the remote's current default
branch. Torii reads it with `git ls-remote --symref` and keeps the answer for
five minutes. Set `git config torii.defaultBase <branch>` in the project clone
to choose another branch. When the remote cannot be read, Torii uses the
upstream of the clone's checked-out branch, then its `HEAD`.

Claude and Codex research workers without a worktree start in job scratch and
receive the project path for reading. Dev and OpenCode workers create a
worktree when needed. The operating system does not enforce read-only access
to the project for research workers.

Provider hosts are detached from the service and hold native session locks.
On restart Torii reattaches to live hosts or resumes the saved native session.
It does not blindly replay uncertain work. Inspect actual effects before
continuing an interrupted task. A provider receipt proves delivery to a session,
not that the agent applied the instruction. A process exit does not prove a
deployment worked; verify the target.

Torii reports Claude and Codex worker processes that survive the provider and
are reaped at turn end. The worker result retains its success and text and
reports process details. Existing failures are preserved.

Each worker receives the job's `results/task-<id>/HANDOFF.md` as it existed
when queued. Before it ends, it should update the file in at most 60 lines with
its goal, current state, completed work and proof paths, work in flight, next
step, work not to repeat, and open questions. `workers.get` and `tasks.get` show
`no_handoff` when the file was missing or older than the worker start. Older
workers without a recorded start have an unknown value. To measure the share of
recorded worker ends with fresh handoffs, run:

```sh
python3 scripts/handoff-share.py /path/to/state.sqlite
```

`coordinator/policy.py` is the source of coordinator instructions.
`coordinator/control_api.py` defines operations and their access rules.
`coordinator/store.py` owns durable state, and `coordinator/mcp.py` exposes
coordinator tools. Discover current arguments with `ctl describe` or MCP tool
schemas rather than relying on a copied operation list.

## Accounts and credentials

Claude profiles retain their native sign-ins. Torii does not read or pass
Claude sign-in tokens. The owner can pick an eligible Claude account; otherwise
Torii selects an eligible account by weekly reset time and alias. Claude
accounts switch at 99% usage or at a configured reserve. A running parent waits
for active work to settle, then resumes its native conversation under the next
account. Workers can switch when Claude rejects them for usage. When no Claude
account is available, new parents can use the active ChatGPT account.
When ChatGPT auto-switch is on, a parent paused for usage continues when any
ChatGPT account becomes available. Any connected agent can run any chat,
including the home chat, and `model.parent` without topic can make any of them
the default. When the agent of a chat or a worker is out of usage, Torii moves
the work to the next agent that has capacity: the other of Claude and ChatGPT
at the same tier. A chat moves when it next starts, and it returns after its
current work settles once its own agent can run again. A worker that has not
started moves at launch. A running worker that hits its limit continues on the
new agent from a handoff: its brief and the `HANDOFF.md` of its job. Each move
sends one short note. When no agent has capacity, the work waits for the first
reset. A picked agent that does not start counts as out for 15 minutes when
another agent takes the chat; the wait doubles with each failure in a row, up
to a day, and a new pick clears it. OpenCode work moves out the same way.
No work moves to OpenCode. A short OpenCode rate limit is not an outage.
A ChatGPT worker moves only while Codex account switching is on.
Codex account switching is off unless
the owner enables it. With it on, workers and ChatGPT chat agents use accounts
with included allowance first, in weekly reset order. Both move at 99% usage.
They then use prepaid credits, largest balance first, down to zero. With
auto-switch off, workers and parents stay on the active account and can use its
credits after its included allowance runs out. An ordinary-meter usage-limit
rejection blocks a funded account until a fresh status shows usable included
allowance or permits credits with a positive balance. A separate model-meter
rejection holds the account until that meter resets.
Torii sends one home notice when work starts using credits during an outage.
`/health` shows each ChatGPT account's credit balance. Account enablement, selection,
models, and delegation are owner controls.

Use `/secrets` to request or manage credentials. The owner fills a private card
in the bot DM. Declared secrets reach authorized workers through their process
environment; the coordinator receives names and metadata. Output scrubbing
has limits. Native transcripts, files, encoded values, and provider requests can
retain secrets. Telegram also retains its own records. If Telegram message
deletion fails, delete the message and rotate the credential.

The bot token, state, vault, and logs are private local data. A worker running
as the same operating-system user can still access files available to that user.
Do not upload logs or transcripts to public issues. Never read or copy account
credential files or provider history during setup or maintenance.

## Operate and recover

Run exactly one service poller for a bot. The installer configures a macOS
LaunchAgent or Linux systemd user unit. `scripts/run-service.sh` runs the
service in the foreground. `--pair-only` still contacts Telegram but does not
run agents. Pairing uses a single-use code and checks the group creator. Never
share that code. Re-pairing disables old topics and retires undelivered reports;
project folders and native sessions remain local.

Use `/accounts` to see usage, add or remove accounts and set models. Ask the
coordinator in text for an account pick or a banked reset. ChatGPT auto-switch
(`accounts.codex_auto`) and Codex on or off (`delegation.codex`) are local
controls. Use `ctl list` and `ctl describe` for local controls. The owner's rules live in rule files; see
Rule files below.

## Job record recovery

Torii keeps job records and journals in the state directory's `records/` folder.
The ledger also holds an allowlist of durable, non-secret settings, including chat tags, and plain copies
of the personal, tag and chat rule files. It does not back up the vault, account credentials,
worker sessions, or chat memory. A setting with a secret-shaped value is omitted.
A rule file with a secret-shaped value blocks the configuration snapshot until the
value is removed. Check the records problems before relying on the backup.

To restore records and the saved configuration into a fresh state directory, run:

```sh
python3 -m coordinator --state-dir /path/to/new-state records restore --source /path/to/records
```

A closed job's journal leaves the folder once it is committed; the record's `log:` names the commit that holds it,
and `tasks.get` prints the `git show` command.

The source can be a records folder or a clone. Restore brings back the saved
`records.sync` choice and remote. When the service starts, an enabled sync can
push later commits to that remote. Pair the bot and reconnect accounts separately;
the vault has no backup in the ledger.

## Rule files

Torii loads `coordinator/defaults/Torii.md`, then `<state>/personal.md`, then
`<state>/tags/<tag>.md` for each of the chat's tags in order, then `<state>/chats/<topic>/chat.md`.
Missing tag files are skipped. The chat agent and its workers see the chat's MCP
server names in their fixed rules. Coordinators load updated rules with the next message.
New workers receive the rules without coordinator-only sections.

New chats start with no tags.

Use `chat.tags topic=<topic>` to show the tags. Set them in order with
`chat.tags topic=<topic> tags=dev_agent,project_torii`, or clear them with `chat.tags topic=<topic> tags=`.
Project tags include `project_torii`, `project_notes`, and `project_webapp`. Tag names start with
a lowercase letter or digit and can contain up to 64 lowercase letters, digits, underscores or hyphens.
`policy.show topic=<topic>` shows each file and its byte count. `policy.set text=...` replaces
`personal.md`. `policy.chat topic=<topic> message=<owner-message-id> append=...` adds a rule to
`chat.md`.

## Chat MCP servers

Each chat can select HTTP MCP server names from the Torii home's
`~/.claude.json`. Torii passes valid HTTP entries and records a problem for
unsupported or invalid entries. For example, run:

```sh
python3 -m coordinator ctl call chat.mcp topic=TOPIC 'names=["example-mcp"]'
```

A tag can name default servers for its chats:

```sh
python3 -m coordinator ctl call tag.mcp tag=TAG 'names=["example-mcp"]'
```

A chat without its own selection uses the union of its tags' defaults, in tag
order. A chat's own selection overrides them; `names=[]` selects no servers and
`names=null` returns the chat to its tag defaults. New chats use their tag defaults.
Claude, OpenCode, and Codex coordinators and workers use their chat's selection
on new launches. The server entries and headers stay in `~/.claude.json`.
The selection applies at the next coordinator or worker launch.
A failed connection leaves that server's tools unavailable.

The chat's selection only adds servers. Claude and Codex still load the MCP
servers from their own configuration: Claude's user, local and project servers
(including `.mcp.json`) and Codex's `config.toml` servers, stdio servers
included. For Claude, a selected server replaces a native server of the same
name. Codex cannot replace a server it already has, so its own server keeps the
name and Torii records a `codex-name-taken` problem.

Torii 2.0 shared the `shared_mcp_servers` names in `local-overrides.json` with
every chat. The first start after an upgrade gives each existing chat that
selection once. Chats created later start with their tag defaults.

Telegram updates are off by default. Ask the coordinator to turn them on
(`service.updates enabled=true`) if wanted. `/update` shows a card before applying the displayed versions. The
updater refuses tracked changes and non-fast-forward checkouts. It runs no tests.
A successful first Telegram poll at the intended commit is its health signal;
failed updates can roll back. Do not infer live success from a process exit.
When `origin/main` moves before the tap, Torii still updates to the commit shown
on the card if `origin/main` contains it.

Before a manual update or uninstall, stop new work and let active turns,
background tasks, and workers settle. Inspect the installed checkout and service
configuration. Stopping the poller alone does not stop detached hosts. Preserve
state, the vault and its key, projects, worktrees, and native history. To
uninstall, unload the service and move only inspected, owner-selected files to
Trash. Verify that the service is absent before moving its checkout.

For a pinned macOS release, `scripts/pin-release.py COMMIT` creates or reuses a
detached release worktree and prints the values and commands to apply. It does
not change or restart the service. Only the owner applies the release. After
activation, check the service start log for the intended commit, release path,
and home path. Preserve the previous release while its hosts still run.

To transfer a native session, inspect its live state first. Wait for active work
and bridge checks to settle. Transfer ownership rather than forking, preserving
the working directory and native session ID. Never run two writers against one
native session. Do not restart the old writer while the service owns it.
Never delete, move, copy, or truncate native account history or authentication.

## Change and verify Torii

Each Claude coordinator chat has its own memory folder at
`<state>/chats/<chat>/memory`. Torii warns when a chat index exceeds 40 lines and asks the
coordinator to trim it. Claude's own 200-line or 25 KB load limit still applies.
Nightly maintenance removes dead file links from chat indexes when their
coordinators are idle. It does not remove memory notes.

Use Python 3.9-compatible standard-library code. Persist each request before
acknowledging it. Never rerun agent execution because delivery failed. Code has
no comments; docstrings and a first-line shebang are allowed. Preserve the
owner, routing, recovery, and one-writer boundaries when changing behavior.
Add meaningful regression coverage for changed behavior.

Run the offline baseline with the service interpreter:

```sh
/usr/bin/python3 ./scripts/verify-local.py
```

Set `TORII_PYTHON` to the service interpreter if it differs. The verifier uses
fresh scratch state, fake Telegram transport, and fake providers. It runs the
unittest suite, compilation, comment check, CLI checks, and `git diff --check`.
Read its `result.json`; a proof directory alone is not a pass. Offline checks do
not verify live Telegram, provider sign-in, or deployment.

Live Telegram checks need task-specific authorization and an isolated bot,
topic, state directory, and native session. Confirm one poller owns the test
bot. Check the group picker, administrator rights, topic migration, sign-in,
and message delivery on the selected test client. Do not restart the running
service, change accounts, or send test messages without authorization.

The public snapshot builder `scripts/public_snapshot.py` exports files from a
committed revision using `scripts/public_snapshot_files.json`. It requires a
private denylist and successful secret scanners, and does not push. Review the
output and its report before any separately authorized publication. Update the
manifest when tracked files change. Public files must not contain private names,
credentials, or private operational details.

For a local export, install Git, gitleaks, and trufflehog. Use a new output
folder and a private, nonempty denylist outside the repository:

```sh
/usr/bin/python3 scripts/public_snapshot.py --commit HEAD \
  --output /absolute/new-snapshot-folder \
  --denylist /absolute/private-identifiers.txt \
  --author-name 'Public Author' --author-email 'public@example.invalid'
```

The builder scans before it initializes the output repository. If a scan fails,
inspect and move the partial output to Trash. Use a fresh output path for a
retry. Publication is a separate owner action. Never force-push a snapshot.

## Security

Agents run with the local user's permissions and can read or change accessible
files and accounts. A Git worktree is not a sandbox. Trust the machine, its
users, and the selected agent configurations.

## License

MIT. See [LICENSE](LICENSE).
