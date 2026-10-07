# Access model

This tool runs local agents with full execution authority. Claude workers use
`--dangerously-skip-permissions`. Codex app-server threads use
`approvalPolicy="never"` and `sandbox="danger-full-access"`. The separate exec
path uses `--dangerously-bypass-approvals-and-sandbox`. They can read and modify what your
OS account can access, including local credentials, repositories, and deployment
accounts. A Git worktree is not a security sandbox.

Persistent coordinator agents and workers both run with unrestricted local tools.
Restricted structured decision calls are a separate, special path with execution
tools disabled. Follow each target repository's instructions
and verify deployment evidence. Agent instructions guide behavior; they do not
create an OS access boundary.

## Telegram access

Pairing binds the numeric owner ID and a group the sender owns. Keep the code
private until used. Only that owner can submit work, attachments, or settings
changes. Other users, bot messages, edited messages, and anonymous-admin posts
do not submit work. Topics stay disabled until bound to a project.

A single-use `startgroup` link or `/pair CODE` is the local proof. The code lasts
30 minutes. Torii also requires `getChatMember` to identify the sender as the
group creator. Names and usernames are display text. The membership update's
performer is a secondary check; a mismatch does not override the creator check.

Only a SHA-256 hash of the code is stored. Successful pairing clears it in the
same transaction as owner, group, and offset persistence. Torii requests deletion
of the pairing message. Re-pairing disables old topics and retires undelivered
reports. Project folders and native sessions remain local.

`--pair-only` prevents agent execution. Fresh setup also holds work until Claude
or ChatGPT connects. Existing groups with no execution key keep running. Reports are sent
only to the paired group, plus explicit setup notices to a group requesting
pairing or adding the bot. Bot removal pauses sends. A second group is refused.
The bot DM accepts credential envelopes and sign-in codes, never work chat.

## Credentials and records

The bot token, SQLite state, and provider logs live outside the repository with
private permissions. Anyone with the bot token can read messages sent to the
bot, including secret-card replies. Never paste it in an agent chat. If exposed,
use `/revoke` in BotFather and enter the replacement only at the hidden terminal
prompt. An armed private secret card takes the next eligible owner message in
the bot DM. General secret intake does not accept values from group topics.
During a pending Claude sign-in, Torii intercepts the owner's code-shaped
message only in the topic allowed by that sign-in. These are separate intake
paths. Credentials travel as Telegram messages before Torii deletes them.
If deletion fails, delete the message yourself and rotate the credential.
Telegram retains its own records outside Torii's control. Logs can contain
project data. Do not upload them to public issues. Workers do not receive Telegram token environment variables, but workers
running as the same OS user could still read its token file. This is a trusted
single-owner tool, not isolation from a malicious worker or local user.

Native Claude/Codex authentication is owned by their CLIs. Torii automatically
switches eligible Claude accounts and can switch Codex accounts when the owner
enables automatic switching. Verify the active identity before account-specific
work. Change shared login state only when no other work uses it. Preserve native session history.

## Failure limits

A service restart interrupts no coordinator or worker process. A worker whose host
died without an exit record becomes interrupted. A restart cannot roll back a deployment or prove that a stopped process had no external effects.
Inspect actual state before continuing. An ambiguous Telegram send may duplicate
a report. Delivery retries do not rerun the underlying task.

## Report a vulnerability

Use GitHub private vulnerability reporting. Never include live tokens, pairing
codes, or raw transcripts.


Claude profiles stay in their native configuration directories. The bridge saves
aliases, paths, cached quota observations, and transcript references in private
local state. Torii never reads, stores or passes Claude sign-in tokens. Each Claude process
runs with its profile folder, and Claude Code signs in and refreshes from it.
Torii removes inherited Claude and Anthropic credential variables from every
Claude launch. It does not copy credential files. Account selection
applies per process; it does not log other terminal agents out. Profile selection
can also change native settings, plugins, and hooks. Register only trusted local
profiles. The bridge does not sandbox those native configurations.
