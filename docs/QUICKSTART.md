# Quick start

Python 3.9 or newer, macOS or Linux. No extra packages: AgentBrain Handoffs uses the standard library only.

## 1. Install and watch the demo (about 60 seconds)

From a clone:

```bash
python3 -m venv .venv
.venv/bin/pip install .
.venv/bin/handoffs demo
```

Or: `pipx run agentbrain-handoffs demo`.

Open http://127.0.0.1:8765/. You should see four agents (Planner, Engineer, Reviewer, Writer), a handful of live handoff cards, and open work with due times. Cards move from Queued to Accepted to Working to Finished. Returned work is closed automatically in the demo.

Speed it up with `--speed 4`. Another port: `--port 9876`. Ctrl+C stops the server and deletes the temporary database.

Nothing on your machine is contacted. The demo adapter simulates turns in a JSON file next to the demo database.

## 2. Your own two-agent loop

In a project folder:

```bash
handoffs init
handoffs agent add planner --provider demo --name Planner
handoffs agent add builder --provider demo --name Builder
handoffs send planner builder "Add a /healthz endpoint that returns 200." \
  --title "Health check" --due-minutes 30
handoffs serve
```

`handoffs serve` runs the delivery engine and the same live page. In another terminal:

```bash
handoffs status
handoffs inbox builder
handoffs tick          # one delivery pass, if you are not already serving
```

The database is `./.handoffs/handoffs.sqlite3` unless you pass `--db` or set `HANDOFFS_DB`. Every agent process, the engine and the MCP server must share that path.

## 3. Work contracts

A send with `--title` is tracked work. The recipient returns a result; the original sender closes it.

```bash
handoffs return MESSAGE_ID --as builder --summary "Added /healthz; curl returns 200"
handoffs close MESSAGE_ID --as planner accepted --note "Ships."
```

`--blocked` on `return` reports a blocker. Close outcomes: `accepted`, `revision`, `blocked`. `--due-minutes` needs `--title` and must be between 1 and 43200.

A `--key` on `send` makes the send idempotent: the same key, sender, recipient and body returns the original message.

## 4. Wire a real agent session

Register the session you already have, then start an MCP server *as that agent* and run the engine.

```bash
handoffs agent add builder --provider command --cwd . \
  --set 'command=["python3", "my_agent.py", "--prompt-file", "{text_file}"]'
# or, experimental:
# handoffs agent add builder --provider codex --endpoint THREAD_ID
# handoffs agent add builder --provider claude-code --endpoint SESSION_ID --cwd .

export HANDOFFS_DB="$PWD/.handoffs/handoffs.sqlite3"
handoffs serve &
handoffs mcp --agent builder
```

Point Claude Code, Codex or Cursor at that MCP command. Snippets: [MCP.md](MCP.md). Adapter settings: [ADAPTERS.md](ADAPTERS.md).

## 5. Pause, allow-lists, outages

```bash
handoffs config enabled false          # hold every send; no attempt is used
handoffs config enabled true
handoffs config connections explicit   # only pairs you `allow`
handoffs allow planner builder
handoffs block planner builder         # queued handoffs on this pair are held
handoffs config outageGuard '{"preset": "openai-codex"}'
handoffs config outageGuard null
```

`enabledAfter now` ignores older inbox messages so a new database does not wake a backlog.

## If something looks stuck

- `handoffs status` — engine last pass, in-flight counts, needs-attention rows.
- `Engine: never ran` means start `handoffs serve` or `handoffs run`.
- HELD on a connection: `handoffs allow SENDER RECIPIENT`.
- UNAVAILABLE: the agent was removed or its endpoint changed. Nothing is redirected; update the agent and send again.
- A taken port: `handoffs serve --port 9876`.
- UNCERTAIN (or an ACCEPTED turn that never moves): the result could not be confirmed, so its recipient stays reserved. If you know how it ended, `handoffs release ID` frees the recipient. Nothing is ever resent.
- Codex and Claude Code turns run inside the engine process that starts them, so a one-shot `handoffs tick` leaves them WAITING for `handoffs run` or `handoffs serve`. Command agents are fine with `tick`: each run is supervised and recorded on its own.
