# Adapters

An agent's `provider` selects the adapter that delivers into its session. The engine asks the adapter four things: is the agent busy (`activity`), may we write (`prepare`), write this turn (`start`), and what became of it (`observe`).

`handoffs agent add ID --provider NAME` refuses setups that could never deliver (a Codex agent without an endpoint, a command agent without a command or URL).

Shipped providers:

| Provider | Status | Class |
| --- | --- | --- |
| `demo` | Stable | `DemoTransport` |
| `command` | Stable | `CommandTransport` |
| `codex` | Experimental | `CodexTransport` |
| `claude-code` | Experimental | `ClaudeCodeTransport` |

The Codex and Claude Code modules load only when asked for. A failure in one leaves the others working; that provider's agents show as unavailable instead of being sent to blindly.

## demo (stable)

Simulated agents. Turns last 4–15 seconds, divided by `--speed` in the demo. About 80% finish, 10% fail, 10% stop, chosen from `(seed, request_id)` so a run is reproducible.

Turn state lives in a JSON file next to the database (`<db>.demo-turns.json`) so the engine, the page and the CLI agree across processes.

When a finished turn was a work assignment, the demo agent returns the work through the store, the same way a real agent would with `handoffs return`.

```bash
handoffs agent add planner --provider demo
handoffs agent add builder --provider demo
```

`handoffs demo` registers Planner, Engineer, Reviewer and Writer this way.

## command (stable)

Run a program, or POST to a URL. No shell is involved.

### argv

`settings.command` is a JSON list. Placeholders in any argument:

| Placeholder | Value |
| --- | --- |
| `{text_file}` | Temp file holding the handoff text |
| `{request_id}` | This delivery's request id |
| `{endpoint}` | The agent's endpoint |
| `{cwd}` | The agent's working directory |

The same values are in the environment as `HANDOFFS_TEXT_FILE`, `HANDOFFS_REQUEST_ID` and `HANDOFFS_AGENT_ID`. `HANDOFFS_AGENT` is set to the recipient and `HANDOFFS_DB` to the engine's database, so the card's `handoffs return ID --as AGENT` line works as written inside the turn. Exit 0 is a finished turn; any other exit is a failure. Default timeout is 3600 seconds (`timeout`).

```bash
handoffs agent add builder --provider command --cwd . \
  --set 'command=["python3", "my_agent.py", "--prompt-file", "{text_file}"]' \
  --set timeout=900
```

Each run has a small supervisor process of its own that records the exit code and the last 20 output lines in `<db>.command-runs/`. The run therefore does not depend on the engine: a one-shot `handoffs tick` may exit and `handoffs serve` may restart, and the next pass still sees how it ended. If the supervisor itself is killed (a reboot, say) the run is reported as interrupted, never guessed as finished.

### HTTP

`settings.url` takes precedence. Each delivery is one POST of JSON `{request_id, text, agent}`. A 2xx response means accepted. Without `status_url` that counts as returned at once. With `status_url` (`{request_id}` is filled in, or added as a query parameter) a GET must return `{status}` as one of `accepted`, `running`, `returned`, `failed`, `stopped` (and a few aliases). A 404 means that request is gone.

```bash
handoffs agent add builder --provider command \
  --set url=http://127.0.0.1:9000/handoff \
  --set status_url=http://127.0.0.1:9000/status/{request_id}
```

## codex (experimental)

Drives `codex app-server` (JSON-RPC 2.0 over stdio, newline-delimited) so a handoff becomes one turn in an **existing** Codex thread. The binary is `settings.codex_bin`, else env `CODEX_BIN`, else `codex`.

```bash
handoffs agent add builder --provider codex --endpoint THREAD_ID
```

`--endpoint` is the Codex thread id. The adapter:

1. Handshakes with `initialize` then `initialized`.
2. Reads the thread; resumes it if it is not loaded.
3. Starts a turn with `turn/start` and keeps **that** app-server process alive until the turn is observed to finish, because the turn runs inside it.
4. Matches the turn by id, or by the handoff marker in the user message text.

Caveats:

- The app-server protocol is young and may change.
- The turn runs inside the app server the engine started, so a one-shot `handoffs tick` never starts a Codex turn; use `handoffs run` or `handoffs serve`. The app server runs with `HANDOFFS_AGENT` set to the recipient.
- Stopping the engine while a turn runs interrupts that turn; resend rules decide what happens next.
- Approval prompts are declined so a turn never hangs waiting. Give delivered threads an approval policy that does not prompt.
- An error mentioning "active writer" or "already" is treated as the owner refusing the write (`OWNER_REJECTED`, one retry).

## claude-code (experimental)

Drives Claude Code headless: `claude -p --resume <session id> --output-format stream-json --verbose`, with the handoff text on stdin, in `agent.cwd`. The binary is `settings.claude_bin`, else env `CLAUDE_BIN`, else `claude`.

```bash
handoffs agent add builder --provider claude-code --endpoint SESSION_ID --cwd /path/to/project
```

`--endpoint` is the Claude session id, `--cwd` is the project folder that session belongs to.

`start` waits for the first `system`/`init` event, then returns. The process's final `result` event is the outcome (`is_error` false → RETURNED). While our process is not running, the session transcript under `~/.claude/projects/` (or `CLAUDE_CONFIG_DIR`) decides: written in the last 90 seconds, or ending in a turn that has not finished (a tool call still running, or a prompt or tool result without a reply), means the owner is using the session (BUSY). An unfinished turn silent for over an hour counts as stopped. The delivery runs with `HANDOFFS_AGENT` set to the recipient.

After a restart, observation scans the transcript for a user message whose handoff marker matches.

## Writing your own

Subclass `agentbrain_handoffs.transport.Transport` and register it on a `Router`. `start` must return a receipt with `turnId`. Raise `NotAccepted` when the owner of the session is writing. See the module docstring on `transport.py` for the full contract.

The engine never sends to an adapter that reports the agent as busy, and it never retries an UNCERTAIN row.
