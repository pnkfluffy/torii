<h1 align="center">
  <img src="docs/brand/torii-mark.png" alt="Torii mark: a gate inside a speech bubble" width="120">
  <br>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/torii-wordmark-dark.png">
    <img src="docs/brand/torii-wordmark-light.png" alt="torii: your agents, one gate away" width="440">
  </picture>
</h1>

<p align="center">
  <b>Run your Claude Code and Codex agents from one Telegram chat.</b><br>
  Like Grok Bot, Cursor, or dots, but on your own machine, with the subscriptions you already pay for. No lock-in.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License: MIT"></a>
  <img src="https://img.shields.io/badge/python-3.9%2B-blue" alt="Python 3.9+">
  <img src="https://img.shields.io/badge/Telegram-bot-26A5E4?logo=telegram&logoColor=white" alt="Telegram">
  <a href="https://x.com/jackson_felty"><img src="https://img.shields.io/badge/follow-%40jackson__felty-000000?logo=x" alt="Follow @jackson_felty on X"></a>
</p>

<!-- DEMO VIDEO: drag a 20-40s mp4 into the GitHub README editor and paste the resulting
https://github.com/user-attachments/assets/... URL on its own line here. -->

## Install

You need Git, Python 3.9+, and the `claude` or `codex` CLI installed and signed in. Then run:

```sh
git clone https://github.com/pnkfluffy/torii.git ~/torii && cd ~/torii && python3 scripts/setup.py
```

Or ask your agent: *"Set up Torii by following https://github.com/pnkfluffy/torii/blob/main/docs/agent-setup.md. Never ask me for my Telegram bot token."*

Setup walks you through BotFather, keeps your bot token out of chats and command lines, installs the
background service, and gives you a link to add the bot to a Telegram group you own.
See [Install and first run](#install-and-first-run) for the full steps.

Torii is a small service that runs on your own Mac or Linux machine. It connects
you, as the one paired Telegram owner, to the Claude and Codex command-line agents
installed on that computer. Each Telegram project topic gets its own long-running Claude or ChatGPT
coordinator. You send a task in plain words; the coordinator plans it, hands
parts to workers, checks their results, and answers in the same topic.

Torii is an independent project. It is not affiliated with Anthropic, OpenAI, or Telegram.

## Why Torii

| | Torii | Hosted agent apps |
| --- | --- | --- |
| Models | Claude Code and Codex; coordinators continue on ChatGPT when every Claude account is out of usage | The vendor's own models |
| Where it runs | Your own machine, your repos and files | The vendor's servers |
| Cost | The Claude and ChatGPT subscriptions you already have | A separate plan |
| Interface | Telegram: one topic per project, voice notes, images, and files | The vendor's app |
| Work tracking | One Git branch and worktree per task; state survives restarts | Varies |
| Source | MIT, single owner, self-hosted | Closed |

Coding agents are good at long jobs, but they live in a terminal. Torii puts
them in the chat app you already carry:

- **Away from the desk.** Start, steer, and review work from your phone.
- **One conversation per project.** Each topic has a separate coordinator
  conversation and job list. `/projects` → **New project** creates its topic
  and a Git folder on your computer.
- **Keeps talking while work runs.** Workers run in the background. You can
  add instructions mid-task and they arrive in order.
- **Survives restarts.** Messages, replies, and job state survive restarts,
  provider switches, and usage-limit rejections. Torii asks the coordinator
  to check interrupted work before continuing.

## Requirements

- macOS 13 or newer, or Linux with a systemd user session, plus Git. On macOS,
  install Git with Apple's
  [Command Line Tools](https://developer.apple.com/documentation/xcode/installing-the-command-line-tools)
  by running `xcode-select --install` in Terminal. On Linux, use your
  distribution's `git` package.
- [Python 3.9 or newer](https://www.python.org/downloads/). Torii's core
  has no third-party Python dependencies. Check `python3 --version` before setup.
- [Claude Code](https://code.claude.com/docs/en/setup), the `claude` command,
  or the [Codex CLI](https://developers.openai.com/codex/cli), the `codex` command.
  Use an account with access to the selected provider and model. Connect Claude
  or ChatGPT in Accounts.
  Either provider can run the main chat.
  Saved secrets and worker goals work with Claude and ChatGPT workers.
- A new Telegram bot made with `/newbot` in @BotFather just for Torii. Using a
  bot that another program polls interrupts that program.
- A computer that stays awake and online. A sleeping computer does not answer.

Voice notes are optional. Local voice transcription is macOS-only and requires macOS 14 or newer
on Apple silicon and installs a separate environment and model on first use.
See [voice setup](docs/setup.md#messages-and-reports) and
[native CLI capability checks](docs/operations.md#native-cli-contract).
Setup checks CLI availability, not a minimum CLI version.

## Install and first run

1. Ask your local agent to set up Torii, using this prompt:

   ```text
   Set up Torii on this computer. Follow the instructions at
   https://github.com/pnkfluffy/torii/blob/main/docs/agent-setup.md
   Never ask me for my Telegram bot token.
   ```

   It clones Torii, checks this computer, guides you through BotFather, and opens
   the setup window in Terminal. If it cannot open Terminal, it gives you one
   command to run there. It never asks for your bot token.

2. Or install by hand:

   Install the Command Line Tools, Python, and at least one provider CLI from the
   official links in [Requirements](#requirements). Open a new Terminal window
   after installation. Check `git --version`, `python3 --version`, and
   `claude --version` or `codex --version` before cloning.

   ```sh
   git clone https://github.com/pnkfluffy/torii.git ~/torii
   cd ~/torii
   python3 scripts/setup.py
   ```

   The token prompt is hidden. Do not put the token in an agent chat or a command.
   The installer saves the absolute path of the Python interpreter that runs
   setup in the LaunchAgent. To select another Python 3.9+ interpreter, replace
   `python3` with its absolute path. Keep that interpreter installed.
   `TORII_PYTHON` overrides the offline verifier's interpreter, not the installer.
   Set it to the saved path when verifying a custom-Python installation.

3. In Telegram, pick a group you own and tap **Add as admin**. If Telegram did
   not open, use the printed link. No group yet? Create one before opening it.
   Keep **Manage topics**, **Delete messages**, and **Pin messages** enabled.

4. If asked, turn on **Topics** under the group name → **Edit**, then tap
   **Check again**. Continue in the **Torii** topic and tap **Add ChatGPT** or **Connect Claude**.
   Follow the sign-in card in your browser; paste a code in that topic if Claude asks.
   Send your first request in **Torii**, then choose its project name.

Each project starts under `~/Projects/torii` and gets its own group topic.
Torii holds work until Claude or ChatGPT is connected. Either can run the main chat.
Existing group installs keep working. The bot DM is only for secrets and sign-in
codes. [Setup reference](docs/setup.md) covers setup and terminal alternatives.

## Daily use

| Action | How |
| --- | --- |
| Give or steer work | Send a message in an enabled project topic. |
| Answer a question | Reply to the coordinator's message. |
| Set a long verification goal | `/goal CONDITION` |
| List or create projects | `/projects`, then **New project**, **Projects folder**, or **This topic ›** |
| Add or inspect accounts | `/accounts`, then **Add Claude** or **Add ChatGPT** |
| Set models and delegation | `/accounts`, then **Models & Codex ›** |
| Catch up on this topic | `/tldr` |
| See running agents and system load | `/health` |
| Manage stored credentials | `/secrets`, then a name and **Rotate**, **Revoke**, or **Ask again** |
| Check the connection | `/ping` |
| See the command list | `/help` |
| Inspect tasks and workers on the computer | `python3 -m coordinator status` |
| List local control operations | `python3 -m coordinator ctl list` |

The command menu has seven commands, all without arguments. `/help` is a plain
list with no buttons. `/setup` and `/start` still handle pairing and project
linking, but stay out of the menu. The full command list is in
[Telegram controls](docs/operations.md#telegram-controls).
See [Update Torii](docs/operations.md#update-torii) and
[Uninstall Torii](docs/operations.md#uninstall-torii) for service maintenance.

## Features

- **A coordinator per topic.** Each project topic has its own persistent
  Claude or ChatGPT coordinator session. Your first request starts it. You can
  [choose the provider and model for all coordinators](docs/control-api.md#setup-and-settings-operations)
  with local controls.
  Changes wait for active work to settle. Provider switches post a notice in
  that topic. Sessions resume after a relaunch.
- **Claude and Codex workers.** The coordinator delegates to native Claude
  subagents, Codex, or background service workers, and checks what they claim.
  The coordinator and workers follow your project's `AGENTS.md` and run the
  project's own checks.
- **One worktree per task.** Each committed task gets its own Git branch
  (`torii/task-<id>`) and worktree. Each finished piece ships on its own branch
  and PR without waiting for unrelated work.
- **Attachments and voice.** Send images, files of any type, and voice notes.
  Voice notes are transcribed locally on the Mac (macOS only).
- **Several accounts.** Use your own Claude and ChatGPT subscriptions. Each
  Claude agent uses one of your Claude Code sign-ins and that account's
  `CLAUDE_CONFIG_DIR` folder. Claude rotation is automatic at 98% used by
  default, or at a local per-account reserve. Torii waits for current work to
  settle, then restarts the coordinator under the next eligible account and
  resumes the same conversation. When every Claude account is out of usage,
  coordinators continue on your ChatGPT account and return to Claude after it
  recovers. Torii reads usage through Claude Code, which
  talks directly to Anthropic. Torii never reads, copies, or forwards sign-in
  tokens. Codex worker account switching stays off until you turn it on.
  ChatGPT coordinator accounts never switch automatically.
- **Private credentials.** Send a credential through a secure card. Torii
  stores accepted values in an encrypted local vault and keeps them out of
  its work-message records and prompts. Tap **Fill privately** to use the bot DM.
  See [credential transport and deletion](docs/setup.md#setup).
- **Settings in Telegram.** `/accounts` shows usage, sign-ins, and resets.
  Its **Models & Codex ›** button opens model and delegation settings.
  `/projects` manages projects. `/health` shows running agents and system load.
- **Status at a glance.** Torii reacts to your message: 👀 when the coordinator
  takes it on, 👨‍💻 or 🤔 when a worker starts coding or researching, and 👌 when
  the job is done.

## How it works

```mermaid
flowchart LR
    T[Telegram project topics] --> M[Saved messages]
    M --> C[Claude or ChatGPT coordinator per topic]
    C --> W[Native subagents, Codex, or service workers]
    W --> C
    C --> O[Telegram outbox]
    O --> T
```

1. The service polls Telegram, accepts only the paired owner, and saves each
   message in SQLite before anything acts on it.
2. It hands each saved message, in order, to that topic's coordinator. Messages
   sent while a turn runs are delivered too.
3. The coordinator answers directly, or commits to a task and delegates it.
   Worker results come back to the coordinator as messages.
4. Replies go through a saved outbox. A delivery retry never reruns an agent.

Every coordinator and worker runs under its own detached host, so restarting
the service interrupts no agent. On relaunch Torii reattaches to live hosts or
resumes each topic's exact native session. It does not blindly rerun work that
may have partly happened. See [Architecture](docs/architecture.md) and
[Coordinator and delegation](docs/orchestration.md).

## Safety model

Torii is a trusted, single-owner tool. Read the [access model](SECURITY.md)
before you enable agents.

- **Agents have your permissions.** Claude workers run with
  `--dangerously-skip-permissions`. Codex app-server threads use
  `approvalPolicy="never"` and `sandbox="danger-full-access"`; the separate
  exec path uses `--dangerously-bypass-approvals-and-sandbox`. They can read and change
  anything your user account can. A Git worktree is not a sandbox.
- **Only you can give work.** An expiring, one-use pairing code binds your
  numeric Telegram ID and the group you own. Other users, bots, edited messages, and
  anonymous admins cannot submit work. New topics stay disabled until bound to a project.
- **Credential handling.** Claude profiles keep their own sign-in. Torii never
  reads or passes Claude sign-in tokens. The bot token, state, and logs live outside the
  repository with private permissions. Credentials go through the private chat
  into an encrypted vault. Claude and ChatGPT job workers receive only the
  secrets their task declares, through their own process environment. The
  coordinator receives names and metadata only. Codex secret workers require
  a private app-server and checked launch controls that disable shell snapshots,
  memory generation, native subagents, and tool-output telemetry. Captured-output
  scrubbing has limits, and native history can retain values a tool prints.
  See [Credentials](docs/operations.md#credentials).
- **No silent reruns.** After a restart, Torii reports uncertain work to the
  coordinator instead of repeating it.

## Docs

- [Setup reference](docs/setup.md): models, effort, usage policy, worktrees
- [Operations](docs/operations.md): controls, accounts, credentials, logs, troubleshooting
- [Architecture](docs/architecture.md): request flow, recovery, stored state
- [Coordinator and delegation](docs/orchestration.md)
- [Control API](docs/control-api.md)
- [Pinned release](docs/release.md)
- [Verification](docs/verification.md): the offline test baseline
- [Access model](SECURITY.md) and [Contributing](CONTRIBUTING.md)

To run the offline checks from this checkout:

```sh
/usr/bin/python3 ./scripts/verify-local.py
```

The verifier defaults to `/usr/bin/python3`. For an installation using another
interpreter, run `TORII_PYTHON=/absolute/path/to/python3 ./scripts/verify-local.py`.
Use the interpreter saved in the LaunchAgent's first `ProgramArguments` entry.
The checks use fake Telegram and provider processes in scratch state and do not touch
a running service.

## Shared MCP servers

Claude coordinators and workers can share MCP servers from your
`~/.claude.json`. The list is opt-in and empty by default; add server names to
the checkout's gitignored `local-overrides.json`:

```json
{"shared_mcp_servers": ["example-mcp"]}
```

See [Setup reference](docs/setup.md#shared-mcp-servers) for the rules.

## License

MIT. See [LICENSE](LICENSE). The Claude and Codex CLIs and services have their
own terms.
