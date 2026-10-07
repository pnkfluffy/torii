# Coordinator and delegation

Each topic has its own persistent Claude coordinator session. Messages carry
the topic ID and project context, and worker results return to that topic's
coordinator. Opening another topic creates a separate conversation. The home
parent receives the list of all enabled topics; other parents receive only their
own topic. Parent processes share the configured coordinator working directory.

The service saves every owner message before it sends 👀 or steers the native
session. It steers messages separately and in order, including messages that
arrive during a turn. A provider receipt proves delivery to the session. The
coordinator checks the resulting work before it claims an instruction was
applied.

A task is a commitment to solve a problem. Questions and brainstorming need no
task. The coordinator creates and updates tasks with MCP tools, gives each task
one worktree, and chooses how to delegate. It may use native Claude subagents,
Codex, or service workers. There is no service worker limit or special
integration worker role. The parent coordinates agents that share a worktree
and keeps one writer per native session.

A service worker result becomes a saved message to the coordinator. The owner
can keep talking while workers run. The coordinator verifies worker claims,
reports progress and results through `telegram.send`, and chooses how to handle
failures. The service records the work and delivers messages; it does not select
repair strategies or retry execution when Telegram delivery fails.

On a relaunch, the service reattaches to each topic's saved coordinator host
and running worker hosts, or resumes that topic's exact native session. It steers
a `restarted` event with open tasks, registered workers, and uncertain input. It does not replay an uncertain
instruction. Restart interrupts no coordinator or worker process. Use `/goal`
when a long task needs extended verification.

See [Architecture](architecture.md) for state transitions and
[Control API](control-api.md) for the available tools.
