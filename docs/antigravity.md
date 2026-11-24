# Antigravity workers and chat agents

Antigravity is optional and off by default. Torii runs Google's official `agy` CLI as
`agy acp --dangerously-skip-permissions`, on Linux and on macOS. Workers run
like Claude and Codex workers: no sandbox by default, declared job secrets
arrive as environment variables, and `gh` uses the owner's login.

## Install agy

Torii uses one fixed path, `~/.local/share/agy/bin/agy`. The binary must be a
regular file owned by the service user and not writable by group or others.
Install it with Google's script, on Linux or macOS:

```sh
curl -fsSL https://antigravity.google/cli/install.sh | bash -s -- --dir ~/.local/share/agy/bin
~/.local/share/agy/bin/agy --version
```

On macOS the script also removes the Gatekeeper quarantine flag. agy updates
itself in place. Turning Antigravity workers on (`delegation.antigravity
enabled=true`), sign-in and every launch refuse with the install command when
the binary is missing or unsafe.

## Add an account

Use **Add account** in Telegram, or `antigravity.signin`. Torii starts
`agy acp` in a new profile home under `~/.local/share/agy/accounts/<alias>`.
agy hands the Google sign-in link to Torii's browser stub (`xdg-open`, and
`open` on macOS), and Torii posts it as a card. Sign in on any device, then
paste the `http://localhost:<port>` address back in the topic. Torii deletes
the message and forwards the address once on loopback.

Each account home is its own `HOME`. agy keeps the login in that profile.
Torii never reads it.

On macOS, a process whose `HOME` is a profile home has no default Keychain.
agy's changelog says it then keeps the login in its own file storage, so each
profile keeps its own login. A Keychain write with no default Keychain can
open a macOS "Keychain Not Found" dialog; watch for it at the first Mac
sign-in. `gh` still finds the owner's Keychain login.

## Sandbox

`antigravity.sandbox enabled=true` puts new workers inside a Linux bubblewrap
write boundary that hides Torii's secrets. It needs Linux and `bwrap`. macOS
refuses the setting, and refuses to start workers while an old state still
has it on.

## Process ownership

On macOS, Antigravity workers run in a launchd resource coalition, like Claude
and Codex workers, so Torii stops their whole process tree at the end. On
Linux they run in an owned process group.
