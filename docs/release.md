# Pinned release

The LaunchAgent that `scripts/install-service.py` writes runs the service from
the development checkout. A commit or branch switch in that checkout changes the
code on disk, and the next KeepAlive start loads that code. A pinned release
runs a fixed commit from its own folder instead.

## Layout

| Path | Content |
| --- | --- |
| `<state>/releases/<commit>/` | A detached Git worktree of the development checkout at one commit. `<commit>` is the first 12 characters of the full commit ID. |
| LaunchAgent `WorkingDirectory` | The release folder. `python3 -m coordinator` loads the code from it, and the MCP server the coordinator starts uses the same code. |
| LaunchAgent `EnvironmentVariables.TORII_HOME` | The development checkout. The coordinator session works in this folder, and the topic bound to it is the home topic. |

`<state>` is `~/.local/state/telegram-agent-coordinator`, or `TORII_STATE_DIR`
when it is set. A release folder never changes after creation. Pin another
commit to get another folder. Keep the previous release folder until the new
one runs well, so a rollback is a plist change.

Without `TORII_HOME`, the service uses its own code folder as home. A release
folder as home would move the coordinator to another project folder, with
other native project settings, and would change the home topic.

## Create a release

```sh
./scripts/pin-release.py COMMIT
```

The script:

1. Finds the development checkout. It uses `--home`, else the `TORII_HOME` or
   `WorkingDirectory` in the current plist, else its own checkout.
2. Resolves `COMMIT` to a full commit ID.
3. Creates `<state>/releases/<commit>/` with `git worktree add --detach`, or
   reuses it when it already holds that commit. It refuses a folder at another
   commit.
4. Compiles the release's `coordinator` package and imports its entry point,
   service, and MCP modules. It uses the interpreter in the plist
   `ProgramArguments`, else `/usr/bin/python3`.
5. Prints the current and new `WorkingDirectory` and `TORII_HOME` values, and
   the commands that would apply them.

The script only reads the plist. It does not change it, and it does not start,
stop, or signal the service.

## Apply a release

Only the owner applies a release. The restart interrupts no coordinator or
worker process. The coordinator host and each running worker host keep the
code of the release that launched them until their processes exit, so keep the
previous release folder until they do. Then run the printed commands. They set
the two plist values with `PlistBuddy`, then run `launchctl bootout` and
`launchctl bootstrap`. launchd keeps the loaded job definition, so a changed
plist takes effect only after `bootstrap`. `service.restart` alone does not
apply it.

After the start, check that the `service start` log line names the release
commit and code folder, and that `home=` names the development checkout.

`scripts/install-service.py` writes a plist for the checkout it runs from,
without `TORII_HOME`. Run it from the development checkout only to return to
the unpinned layout.

## Remove an old release

When no service runs from it, remove an old release with
`git -C <development checkout> worktree remove <state>/releases/<commit>`.

## Updates from Telegram

`/update` is off by default. In **/accounts → Models & Codex**, tap **Turn
Telegram updates on**. The local owner control is:

```sh
python3 -m coordinator ctl call service.updates enabled=true
```

Use this feature with the checkout that the LaunchAgent runs. Keep the pinned
release workflow below when release folders must remain immutable.

The owner receives one card with up to ten commit subjects from `origin/main`,
the remaining commit count, and each out-of-date CLI version. **Update** applies
the displayed versions. **Cancel** changes nothing. If the remote changes after
the card appears, Torii asks the owner to open `/update` again.

The updater drains queued Telegram messages, fetches `origin/main`, and verifies
that the remote and checkout still match the card. It runs no tests, comment
check, or compilation and creates no verification worktree.
Tracked changes, ahead and diverged checkouts, and checkouts outside the tracked
`main` branch are refused. Untracked files do not count as dirty.

Torii fast-forwards its checkout with `git merge --ff-only`. Local npm updates
use a sibling staging prefix copied from `package.json`, exact CLI versions, and
optional dependencies. Torii checks every CLI with `--version`, atomically swaps
`node_modules` and manifests, and retains the previous runtime until a healthy
restart. Failed installs roll back the whole update without network access.

Torii uses a verified local `npm-cli.js` from Homebrew, system paths, or beside
the resolved Node binary. Each install has a one-hour ceiling for hung processes.
The card names the tool during downloads. The next update removes leftover
staging folders with Torii's own prefix. Tools from other installers require a
host update.

A copied watchdog arms before the first mutation. The updater requests the
existing outbox-gated restart and records its ownership before shutdown starts.
The service manager starts the new service. The watchdog can kill the old service
only after its 50-second shutdown grace, with the same PID and process start time.
The health deadline also respects this grace. It never signals parent or worker hosts.

The health signal is the new service's first successful Telegram poll. That
service stores its startup commit, PID, and process start identity in the pending
update record. The poll also identifies the update's own restart. The watchdog
requires that restart, the expected commit, and a different live
PID within 240 seconds. A CLI-only update skips checkout checks and the merge,
then follows the same restart and health protocol.

If the health deadline expires, the watchdog restores attempted CLI versions
from the saved runtime directories and manifests without npm or network access,
resets an unchanged clean checkout with `git reset --keep`, and restarts the
LaunchAgent on macOS or the systemd user unit on Linux. It waits another 240 seconds
for a new service to poll at the old commit. If another deploy moves HEAD to a
different commit, the watchdog stands down without rollback. The service edits the original
card with the outcome and each restoration result. An unrelated restart cannot
count as update success. The restart lock holds only while the named watchdog's
PID and process start time match a live process. A dead watchdog record settles
from the checkout, running commit, restart identity, and observed tool versions.
Turning updates off also settles a dead record. A new service clears only this
update's leftover restart request. Each npm subprocess records its identity
before it starts installation and has its own process group.

The record is `telegram_update_v1` in the service database. Existing
`<state>/update-checks/` folders from older versions remain untouched.
Record writes use SQLite transactions and compare
the current value and phase before changing it. The copied watchdog
and its private log remain under `<state>/update-watchdogs/<update-id>/`.

Deploying CLI fingerprints relaunches each existing parent once when idle.
Subsequent CLI changes do the same. Active turns and native background work
settle first. Native session IDs and history remain in place. Fingerprints apply
even when Telegram updates are off.
