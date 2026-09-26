# AgentBrain Handoffs

Exact, durable handoffs between AI coding agents. Each handoff is one new turn in the recipient's existing session.

```
pip install .
handoffs demo
```

Open the URL it prints (http://127.0.0.1:8765/). Four simulated agents hand work to each other. Ctrl+C stops and deletes the demo data.

## Why this exists

A team of coding agents already has sessions, inboxes and tools. What it lacks is delivery: a message that reaches the right session, waits if that agent is busy, is sent once, and is retried within a bound when the provider drops it.

Without that, handoffs get lost, delivered twice, or pasted into a session that is already in the middle of a turn. AgentBrain Handoffs is the delivery layer. It is a local Python package (Python 3.9+, standard library only) with a command `handoffs`, a live page, and an MCP server.

## 60-second quickstart

From a clone of this repository:

```bash
python3 -m venv .venv
.venv/bin/pip install .
.venv/bin/handoffs demo
```

Or, once the package is on PyPI:

```bash
pipx run agentbrain-handoffs demo
```

You should see:

```
AgentBrain Handoffs demo
  Open http://127.0.0.1:8765/
  Four simulated agents (Planner, Engineer, Reviewer, Writer) hand work to each other.
  Nothing real is contacted. The demo data is temporary and deleted when you stop (Ctrl+C).
```

The page updates every few seconds. Handoffs move through Accepted → Working → Finished. Work is returned and closed on its own. `--speed 4` makes the simulated turns finish faster.

To keep the demo on another port: `handoffs demo --port 9876`.

A longer walkthrough, including your own agents, is in [docs/QUICKSTART.md](docs/QUICKSTART.md).

## Concepts

A **handoff** is a message addressed to one registered agent. The delivery engine turns it into exactly one new turn for that agent, at the session it had when the handoff was enrolled.

A **work contract** is a handoff with a title. The recipient returns a result (or a blocker); the sender closes it as accepted, revision, or blocked. A due time is optional; if it passes, the sender gets one non-waking reminder.

Delivery states, in plain words:

| State | Meaning |
| --- | --- |
| WAITING | Queued. Next engine pass will try to send it. A provider outage stays here too: the detail explains the hold and no attempt is used. |
| BUSY | The recipient is in the middle of a turn. This waits. |
| HELD | Blocked on purpose: the connection is blocked or delivery is paused. No attempt used. |
| UNAVAILABLE | The agent or its session is gone. Nothing is redirected. |
| OWNER_REJECTED | The session refused the write. One retry is allowed. |
| SENDING | The write is in flight. |
| UNCERTAIN | The write may have landed; the reply was lost, or an accepted turn could not be observed for 10 minutes. Observed, never resent. |
| ACCEPTED | The recipient's session took the turn. |
| RUNNING | The turn is in progress. |
| RETURNED | The turn finished. |
| FAILED | The turn failed or was interrupted, or the session refused it twice (then it was never delivered). |
| ACKNOWLEDGED | The recipient read the message; no separate turn is needed. |
| CANCELLED | Proven absent after enough history scans, or released by a person with `handoffs release`. Not resent. |
| DUPLICATE | Identical to a handoff still in progress. |

## Guarantees

1. **Exact recipient.** A handoff is delivered only to the agent it was addressed to, at the endpoint it had when enrolled.
2. **One write per attempt.** The attempt is reserved before sending. A lost reply goes to UNCERTAIN and is reconciled by observation. A refused write is retried once; a second refusal ends the delivery as FAILED, so it never holds the recipient.
3. **Never interrupt.** A busy recipient keeps its turn. A second handoff to the same agent waits as BUSY.
4. **Bounded resend.** An errored turn is resent up to twice; an interrupted turn once, after ten minutes, and only while the work is still open. Each resend has a new request id.
5. **Honest release.** An UNCERTAIN delivery that three complete history scans over an hour prove was never received is CANCELLED. An accepted turn nobody can observe for 10 minutes becomes UNCERTAIN, so the same rule applies. When you know how a stuck delivery ended, `handoffs release ID` cancels it; nothing is resent either way.
6. **Outages hold.** A provider status-page guard can hold delivery without spending an attempt.
7. **Deadlines.** Work past its due time alerts the sender once, as a non-waking notification.

## Adapters

| Provider | Status | What it talks to |
| --- | --- | --- |
| `demo` | Stable | Simulated agents. Used by `handoffs demo`. |
| `command` | Stable | An argv (no shell) or an HTTP POST. |
| `codex` | Experimental | `codex app-server` JSON-RPC over stdio. |
| `claude-code` | Experimental | Claude Code headless (`claude -p --resume`). |

See [docs/ADAPTERS.md](docs/ADAPTERS.md) for settings, examples and the experimental caveats.

## MCP

Each agent session launches its own MCP server. The identity is fixed at start, so a tool call cannot act as another agent.

**Claude Code**

```bash
claude mcp add handoffs -- handoffs mcp --agent me
```

Replace `me` with the agent id you registered (`handoffs agent add me --provider claude-code ...`).

**Codex** (`~/.codex/config.toml`)

```toml
[mcp_servers.handoffs]
command = "handoffs"
args = ["mcp", "--agent", "me"]
```

**Cursor** (MCP servers in Cursor settings)

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

Point every process at the same database with `--db` or `HANDOFFS_DB`. The engine (`handoffs serve` or `handoffs run`) is what actually delivers; the MCP server only writes to the inbox. Details: [docs/MCP.md](docs/MCP.md).

## CLI

```
handoffs init
handoffs agent add planner --provider demo
handoffs agent add builder --provider demo --name Builder
handoffs send planner builder "Add a health check" --title "Health check" --due-minutes 30
handoffs serve                          # engine + live page on http://127.0.0.1:8765/
handoffs inbox builder
handoffs read ID --as builder
handoffs return ID --as builder --summary "Health check is green"
handoffs close ID --as planner accepted
handoffs status --json
handoffs tick                           # one delivery pass (Codex and Claude Code turns wait for run or serve)
handoffs release ID                     # stop tracking a stuck delivery; never resent
handoffs run                            # engine only
handoffs mcp --agent builder            # stdio MCP server
```

`--db PATH` or `HANDOFFS_DB` selects the database (default `./.handoffs/handoffs.sqlite3`). `--json` is accepted on the commands that print records. Usage errors exit 2.

| Command | Purpose |
| --- | --- |
| `init` | Create the database. |
| `agent add\|list\|remove` | Register agents. `--set KEY=VALUE` is JSON when it parses. |
| `allow` / `block` | Directed connections. |
| `config KEY VALUE` | `enabled`, `enabledAfter`, `connections`, `outageGuard`. |
| `send FROM TO MESSAGE` | Inbox message; `--title` makes it work. `--key` is idempotent. |
| `inbox` / `read` / `accept` / `return` / `close` | Work lifecycle. `--as` or `HANDOFFS_AGENT`. |
| `status` / `tick` / `run` / `serve` / `mcp` / `demo` | Inspect and deliver. |
| `release ID` | Cancel one stuck delivery (for example UNCERTAIN) so its recipient is free. Nothing is sent. |

## Security

The live page binds to loopback and refuses non-loopback `Host` headers (DNS-rebinding). `POST /api/send` needs the token in `<db>.token` (mode 0600). `--public-demo` exists only on `handoffs demo` (simulated agents, temporary data); that page cannot send and shows plain state sentences instead of adapter errors. MCP identity is the `--agent` you started with. Full model: [docs/SECURITY-MODEL.md](docs/SECURITY-MODEL.md).

## Roadmap

- Keep the demo and command adapters stable.
- Harden the Codex and Claude Code adapters as those app protocols settle.
- More status-page presets for `outageGuard`.
- The hosted product at [agentrooms.io](https://agentrooms.io) uses this same delivery contract.

## License

Apache-2.0. Copyright KE Studios.

Source: [github.com/willykeenan/agentbrain-handoffs](https://github.com/willykeenan/agentbrain-handoffs). Live demo: [huggingface.co/spaces/willykeenan/agentbrain-handoffs](https://huggingface.co/spaces/willykeenan/agentbrain-handoffs).

AgentBrain Handoffs is the open delivery layer of AgentBrain (agentrooms.io), the hosted brain for agent teams.
