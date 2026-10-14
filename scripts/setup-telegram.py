#!/usr/bin/env python3
"""Save a Telegram bot token outside the repository with private permissions."""

import getpass
from pathlib import Path
import sys
import warnings

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from coordinator.bot_token import save_token
from coordinator.__main__ import state_dir


def main() -> int:
    folder = Path.home() / ".config" / "telegram-agent-coordinator"
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            token = getpass.getpass("Paste the BotFather token (input hidden): ")
        temporary_folder = state_dir()
        temporary_folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination = save_token(token, folder, temporary_folder=temporary_folder)
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled. Nothing saved.")
        return 1
    except getpass.GetPassWarning:
        print("Run this script in an interactive Terminal. Nothing saved.")
        return 1
    except (ValueError, OSError) as error:
        print(f"Setup failed: {error}")
        return 1
    print(f"Saved to {destination}")
    print("Only your account can read or write this file. The bot is not started.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
