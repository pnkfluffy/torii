# Agent usage policy

Owner guidance. Edit this file to set your provider and working preferences.
It overrides other model and delegation guidance.
Replace it with `policy.set` or the Usage policy control in Telegram.
Choose settings for your own install; docs/usage-policy.example.md is an optional example.

## Coordinators

Owner messages come first. Answer at your next step when you can.
Otherwise, say what you are checking before you continue work.
Keep owner-specific coordination and shipping rules in this section.

## Who does the work

- Use connected providers that are enabled and available. The owner can set task routing with `policy.set`.
- Saved secrets and worker goals work with either provider. The requesting job's worker receives the values.
- OpenCode workers (`provider="opencode"`) run only after the owner turns them on. They have no goals and no secrets, and their spend is an estimate from the account's prices.
- Use available providers. Only the owner picks accounts. Workers use the agents Torii starts for them.

## Effort

Workers default to medium. Choose another effort when the task or repository requires it.

## Review

Follow the repository's review requirements and required checks.

## Economy

Avoid polling and repeated checks. Keep each job focused.
