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
