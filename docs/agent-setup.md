# Set up Torii for an owner

Follow this page only when the owner asks to install Torii. Contributor work
uses AGENTS.md. Do each step in order.

## Protect the token and existing history

Never ask for, accept, read, print, or store the Telegram bot token. Never read
`~/.config/telegram-agent-coordinator/bot-token`, Keychain items,
`.credentials.json`, `auth.json`, or `.env` files. Do not sign in to Claude or
ChatGPT for the owner. Do not run nested `claude -p` or `codex exec`. Preserve
all native account homes and history.

If the owner pastes a token into the agent chat, say:

> That token is now in this chat's history. In @BotFather send /revoke, pick
> the bot, and use the new token only at the terminal prompt.

Continue without using that value. The owner enters the new token with hidden
input in their own Terminal window. Do not capture that window's keystrokes,
scrollback, or output. Never run `scripts/setup.py` through the agent's terminal
or a pseudo-terminal. The installer refuses input unless both stdin and stdout
are real terminal streams. It takes no token argument or environment variable.

## Check the computer

The core service requires macOS 13 or newer, Git, Python 3.9 or newer, and at
least one native provider CLI. It has no third-party Python dependencies.
Other platforms are unsupported. Voice is optional and requires macOS 14 or
newer on Apple silicon, with a separate environment and model download on first use.

Run `git --version`. If Git is missing, have the owner install Apple's
[Command Line Tools](https://developer.apple.com/documentation/xcode/installing-the-command-line-tools)
with `xcode-select --install` in their Terminal. Wait for installation to finish.

Run `python3 --version` and `/usr/bin/python3 --version`. Select a Python 3.9+
interpreter and record its absolute path. If needed, use the
[official Python macOS installer](https://www.python.org/downloads/macos/).
The GUI setup launcher uses `/usr/bin/python3`. If you select another interpreter,
give the owner the terminal command below with that interpreter's absolute path.
The service installer saves the invoking interpreter, `sys.executable`, in the
LaunchAgent. It does not select Python from `TORII_PYTHON`.
The offline verifier defaults to `/usr/bin/python3`; `TORII_PYTHON` overrides
that default. Set it to the saved service interpreter when verifying the checkout.

Check `claude --version` and `codex --version`; at least one must succeed.
If the owner chooses Claude and it is missing, show this command from the
[official Claude Code setup page](https://code.claude.com/docs/en/setup), and
wait until they have installed it:

```sh
curl -fsSL https://claude.ai/install.sh | bash
```

If the owner chooses ChatGPT, follow the
[official Codex CLI installation instructions](https://developers.openai.com/codex/cli).
Have the owner confirm that their account can use the selected provider and model.
Torii does not enforce a minimum CLI version. Setup checks `--version` and the
Codex helper installation, while native startup checks protocol capabilities.
See [the native CLI contract](operations.md#native-cli-contract) for the exact
requirements and the scope of the pinned Codex test. A version check alone
does not prove that sign-in, goals, or secret delivery works.
Do not launch either agent to do this setup.

## Get the checkout

Use `~/torii`, unless the owner gives another folder. Do not clone inside
`~/Projects/torii`, the default project root. If the target is already a Torii
clone, use `git pull --ff-only`. If the path contains anything else, choose a
separate folder. Do not replace it.

```sh
git clone https://github.com/pnkfluffy/torii.git ~/torii
```

## Guide BotFather

Tell the owner to do these steps in Telegram:

1. Open https://t.me/BotFather?startapp to create a bot, or open @BotFather and send /newbot.
2. Send `/newbot`. Send a display name, such as "My Torii".
3. Send a username ending in `bot`, such as `my_torii_bot`.
4. Copy the token that BotFather sends. Do not paste it in the agent chat.

## Open setup in the owner's terminal

On a Mac with a GUI login session, open the setup window yourself:

```sh
open -a Terminal ~/torii/scripts/setup.command
```

Use the actual absolute clone path if it differs. Quote paths with spaces.
This uses LaunchServices. Do not use `osascript`; no Automation permission
prompt is needed. The launcher changes to the checkout and executes
`/usr/bin/python3 scripts/setup.py`. Tell the owner to enter the token there.
Do not attach to or inspect this window.

If `open` fails, including inside an agent sandbox, or the Mac is accessed
over SSH, give the owner this one command to run in their own
terminal:

```sh
cd ~/torii && python3 scripts/setup.py
```

Replace `python3` with the selected interpreter's absolute path if needed.
Use that same interpreter for `scripts/setup.py --status` at handoff.

The installer starts the service and opens the group link. Tell the owner to
pick a group they own and tap **Add as admin**. Keep Manage topics, Delete
messages, and Pin messages enabled. If Topics are off, turn them on in the group
settings and tap **Check again**. Continue in **Torii**, tap **Connect Claude** or **Add ChatGPT**,
and authorize in the browser. For Claude, the owner pastes the sign-in code in that topic;
Torii deletes it at once. Do not inspect the code. ChatGPT uses its device-code card.
Agents turn on when Claude or ChatGPT is ready.
Use **New project** in `/projects` to create another project topic. The hidden
`/project new NAME` form still works in General. For accounts and models, open
`/accounts` and use **Add Claude**, **Add ChatGPT**, or **Models & Codex ›**.

## Finish the handoff

After the owner says setup is done, run `python3 scripts/setup.py --status`.
This mode works without a TTY and reads no token value. It reports token saved,
bot username, paired, mode, agents, service running, both connection states, and the main chat provider.
It prints no numeric owner or chat ID. Report that output, then say:
"Everything else happens in the Torii topic in your group." Stop. Do not drive Telegram.
