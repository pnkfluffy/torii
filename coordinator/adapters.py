"""What each worker provider can do. Core code reads these capabilities instead of testing names.

label: the name the owner sees.
switched: off until the owner sets '<provider>_enabled' to True.
goals: 'native' when the provider has its own goal call, 'steer' when a goal is sent as a
/goal steer, None when it takes no goal.
secrets: takes vault secrets.
plugins: the chat's plugin switches apply.
requeue_lost: a host lost on restart is requeued on its saved session.
keeps_session: a move to another model of the same provider keeps the native session.
steer: 'native' when arrival is read from the transcript, 'queued' when the queue receipt is final.
early_session: the native session exists before the first turn ends.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Caps:
    label: str
    switched: bool
    goals: object
    secrets: bool
    plugins: bool
    requeue_lost: bool
    keeps_session: bool
    steer: str
    early_session: bool


CAPS = {
    'claude': Caps('Claude', switched=False, goals='steer', secrets=True, plugins=True, requeue_lost=True,
                   keeps_session=True, steer='native', early_session=False),
    'codex': Caps('ChatGPT', switched=False, goals='native', secrets=True, plugins=True, requeue_lost=True,
                  keeps_session=True, steer='native', early_session=True),
    'antigravity': Caps('Antigravity', switched=True, goals=None, secrets=True, plugins=False, requeue_lost=True,
                        keeps_session=False, steer='queued', early_session=False),
    'opencode': Caps('OpenCode', switched=True, goals=None, secrets=False, plugins=False, requeue_lost=False,
                     keeps_session=False, steer='queued', early_session=False),
}
LABELS = {provider: caps.label for provider, caps in CAPS.items()}
