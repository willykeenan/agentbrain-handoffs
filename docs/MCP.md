# MCP server

`handoffs mcp --agent ID` is a stdio MCP server (newline-delimited JSON-RPC 2.0) that acts as **that agent only**. Start one server per agent session. A tool call cannot change who it is: there is no `from` / `as` argument, and those names are refused if a model invents them.

The server writes to the shared database. It does not deliver. Run `handoffs serve` or `handoffs run` in the same working tree (same `--db` / `HANDOFFS_DB`) so each handoff becomes one turn.

Protocol versions accepted: `2024-11-05`, `2025-03-26`, `2025-06-18`. Anything else is answered with `2025-06-18`.

## Tools

Identity `{me}` in every description is the `--agent` you started with.

| Tool | What it does |
| --- | --- |
| `send_handoff(to, message, title?, due_minutes?, key?)` | Send a message, or tracked work if `title` is set. |
| `inbox(unread_only=true)` | List messages sent to you. |
| `read_message(id)` | Full message. Marks it read when you are the recipient. |
| `accept_work(id)` | Optional; returning also counts as accepting. |
| `return_work(id, summary, evidence?, blocked?)` | Return a result or a blocker to the sender. |
| `close_work(id, outcome, note?)` | Sender-only. `outcome`: `accepted`, `revision`, `blocked`. |
| `handoff_status(id?)` | One delivery, or your recent handoffs. |
| `list_agents()` | Registered ids, so `to` is exact. |

Errors come back as tool results with `isError: true` and a plain sentence (unknown agent, blocked connection, only the assignee can return, and so on). The same `Store` rules as the CLI apply.

Reading a message sent to you marks it read, so the engine will not start a separate turn for it. Returning work is a separate step; ending a turn does not close anything.

## Claude Code

```bash
handoffs agent add me --provider claude-code --endpoint SESSION_ID --cwd /path/to/project
claude mcp add handoffs -- handoffs mcp --agent me
```

Use the same agent id you registered. If the `handoffs` binary is not on Claude Code's PATH, pass the absolute path of the executable.

To share one database across terminals:

```bash
export HANDOFFS_DB=/path/to/team/.handoffs/handoffs.sqlite3
claude mcp add handoffs -- handoffs mcp --agent me --db "$HANDOFFS_DB"
```

## Codex

`~/.codex/config.toml`:

```toml
[mcp_servers.handoffs]
command = "handoffs"
args = ["mcp", "--agent", "me"]
```

With an explicit database:

```toml
[mcp_servers.handoffs]
command = "handoffs"
args = ["--db", "/path/to/team/.handoffs/handoffs.sqlite3", "mcp", "--agent", "me"]
```

`--db` is accepted before or after the subcommand.

## Cursor

In Cursor MCP settings:

```json
{
  "mcpServers": {
    "handoffs": {
      "command": "handoffs",
      "args": ["mcp", "--agent", "me"]
    }
  }
}
```

Add `"--db", "/path/to/team/.handoffs/handoffs.sqlite3"` to `args` when the working directory is not the project that owns the database.

## A working loop

1. `handoffs init` and `handoffs agent add` for every participant.
2. `handoffs serve` (engine + page) or `handoffs run` (engine only), left running.
3. Each coding agent starts `handoffs mcp --agent <its id>`.
4. An agent calls `send_handoff` / `return_work` / `close_work`. The engine delivers the card into the recipient's existing session when that session is free.

Unknown `--agent` is refused before the protocol starts (`handoffs: Unknown agent …`).
