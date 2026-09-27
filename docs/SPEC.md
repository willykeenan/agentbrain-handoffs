# AgentBrain Handoffs: build spec

What we're building: **AgentBrain Handoffs**, an open-source Python package (import `agentbrain_handoffs`, command `handoffs`) that delivers work between AI coding agents. Each handoff is exactly one new turn in the recipient's existing session. It never goes to the wrong agent, never double-sends, never interrupts a busy agent, and resends automatically within a bound. It comes with readable cards, deadlines, a live web page and an MCP server.

Constraints:
- Python 3.9+, **standard library only**, macOS and Linux.
- License Apache-2.0. Copyright "KE Studios".
- Never reference private paths, people or services from the author's machine: no absolute home paths, no emails, no tokens, and no names of internal tools.

## Already written (the contracts; read these first, change only if a test proves a bug)
- `src/agentbrain_handoffs/store.py`: `Store` covers SQLite agents, connections, messages (inbox), work contracts, handoff rows, events and settings. Read every public method.
- `src/agentbrain_handoffs/transport.py`: the adapter contract (`Transport`, `NotAccepted`, `Router`, `default_router()`). Read the module docstring rules.
- `src/agentbrain_handoffs/cards.py`: cards, `envelope()`, `marker_matches()`.
- `src/agentbrain_handoffs/engine.py`: `Engine(store, transport, clock, monotonic, outage, health_path)` with `tick()` and `run_forever()`. It carries the 7 guarantees listed in its docstring.

Delivery states: PENDING (WAITING, BUSY, HELD, UNAVAILABLE, OWNER_REJECTED), ACTIVE (SENDING, UNCERTAIN, ACCEPTED, RUNNING) and TERMINAL (RETURNED, FAILED, ACKNOWLEDGED, CANCELLED, DUPLICATE).

Agent dict (from `Store.agent`): `{id, name, provider, endpoint, cwd, settings (dict), created, updated}`. `provider` selects the adapter.

## Parts to build (each owner writes only its files)

### A. Engine tests: `tests/test_engine.py`, `tests/test_store.py`
Write a `FakeTransport` with scripted activity, start results, exceptions and observations. Prove every guarantee in `engine.py`'s docstring:
- the exact recipient only
- UNAVAILABLE when the endpoint changes
- one write per attempt, with atomic reservation under two concurrent `tick()`s (use two Engine objects on one DB)
- a lost reply goes to UNCERTAIN and is never resent
- a busy recipient goes to BUSY
- only one in-flight delivery per recipient (a second handoff waits BUSY)
- NotAccepted leads to OWNER_REJECTED, then exactly one retry
- resend: errored is resent 2×, interrupted 1× only after 10 min, never when the work is returned, closed or read; each resend has a new request_id
- proven-absent: CANCELLED after 3 scans spanning ≥1 h, never resent
- the outage hold spends no attempt
- overdue alerts once, as a non-waking notification
- duplicate suppression
- a body change leads to FAILED
- HELD when the connection is blocked or delivery is paused
- `enabledAfter`: older messages are never enrolled
- idempotent send keys
- work lifecycle permissions (only the recipient returns, only the sender closes)
- the envelope contains a card plus a marker that `marker_matches` accepts

Also cover store validation.

You may fix real bugs in `engine.py`/`store.py`. List every change in `docs/ENGINE-NOTES.md`.

### B. Demo and command adapters: `src/agentbrain_handoffs/transports/__init__.py`, `transports/demo.py`, `transports/command.py`, `tests/test_transports_basic.py`
- `transports/__init__.py`: `available() -> {'demo': DemoTransport, 'command': CommandTransport, 'codex': CodexTransport, 'claude-code': ClaudeCodeTransport}`. Import the codex and claude modules lazily so a failure in one never breaks the others.
- `DemoTransport(store=None, state_path=None, seed=0, speed=1.0, clock=time.time)`:
  - Simulated agents. Turns persist in a JSON file (default: next to the store DB, `<db>.demo-turns.json`) so separate processes agree.
  - `start` creates a turn (id uuid4, status open) lasting 4–15 s ÷ speed. Its outcome is finished about 80%, failed 10% and stopped 10%, chosen deterministically from (seed, request_id).
  - `activity` reports the agent's latest turn: open, finished, stopped or failed. It returns `finished` if the agent has none.
  - `observe` maps open→RUNNING, finished→RETURNED, failed→FAILED and stopped→FAILED with `receipt.turnOutcome='interrupted'`.
  - When a turn finishes and the delivered text was a work assignment, the demo agent returns the work through `store.return_work`. It parses the message id from the `Message: <id>` line after `HANDOFF`, and uses a plausible one-line summary drawn from a small list.
- `CommandTransport`: `agent.settings.command` is an argv list with placeholders `{text_file}`, `{request_id}`, `{endpoint}`, `{cwd}`.
  - `start` writes the text to a temp file, runs the command (no shell, timeout `settings.timeout`, default 3600) in a background thread, and returns `{'turnId': request_id, 'clientUserMessageId': request_id}` right away.
  - `activity` reports open while the agent's command is running.
  - `observe` gives RUNNING, then RETURNED on exit 0 or FAILED otherwise, with the exit code and last 20 output lines in the receipt.
  - Optional `settings.url`: POST JSON `{request_id, text, agent}` instead. 2xx means ACCEPTED, which is RETURNED at once unless `settings.status_url` is given (a GET returning `{status}`).

### C. Real agent-app adapters: `transports/codex.py`, `transports/claude_code.py`, `tests/test_transports_real.py`
Both are marked **experimental** in docstrings. All tests use fake executables (a Python script that speaks the protocol). Nothing may call a real `codex` or `claude`.
- **`CodexTransport(binary=None)`** drives `codex app-server` (JSON-RPC 2.0 over stdio, newline-delimited). The binary comes from `settings.codex_bin`, env `CODEX_BIN`, else `codex`.
  - Handshake: `initialize` with clientInfo, then the `initialized` notification.
  - `activity`: `thread/read {threadId: endpoint, includeTurns: false}`. A status type of `active` means open. `idle`/`notLoaded` means ask `thread/turns/list {threadId, limit: 1, sortDirection: 'desc'}`: completed→finished, failed→failed, interrupted→stopped, inProgress→open, none→finished. Any error means unknown.
  - `prepare`: `thread/read` must return the same id. If `notLoaded`, call `thread/resume {threadId}`. An error mentioning "active writer" or "already" raises `NotAccepted`.
  - `start`: `turn/start {threadId, input: [{type: 'text', text}], clientUserMessageId: request_id}`. Return `turnId = result.turn.id`. **Keep that app-server process alive** until `observe` sees the turn finish, because the turn runs inside it. Keep one process per endpoint.
  - `observe`: page `thread/turns/list {threadId, limit: 10, sortDirection: 'desc', cursor}` (at most 20 pages). Match by receipt turnId, or else by `cards.marker_matches` over userMessage item text. inProgress→RUNNING, completed→RETURNED, failed→FAILED, interrupted→FAILED plus `turnOutcome: 'interrupted'`. If every page is scanned with no match, return `receipt.historySearch = {exhausted: True, candidate: None, turnId: None}`.
- **`ClaudeCodeTransport(binary=None)`** drives Claude Code's headless mode. The binary comes from `settings.claude_bin`, env `CLAUDE_BIN`, else `claude`.
  - `start` runs `claude -p --resume <endpoint session id> --output-format stream-json --verbose` with the text on stdin, as a background process in `agent.cwd`. It returns `turnId = <request_id>` once the process has started and printed its first `system`/`init` event. Otherwise it raises `NotAccepted` if the process exits non-zero before any output.
  - `activity`: open while our delivery process runs. Otherwise the session transcript (`~/.claude/projects/*/<session id>.jsonl`; honor env `CLAUDE_CONFIG_DIR`) counts as open if modified within the last 90 s (someone is using it interactively), and finished if older.
  - `observe`: the process's final `result` event. `is_error` false→RETURNED, true→FAILED. While running→RUNNING. For recovery after a restart, scan the transcript for a user message that `marker_matches`.

### D. Web page: `src/agentbrain_handoffs/web.py`, `src/agentbrain_handoffs/page.html`, `tests/test_web.py`
`serve(store, engine=None, host='127.0.0.1', port=8765, public_demo=False)` runs on stdlib ThreadingHTTPServer. When an engine is given, it runs `engine.run_forever` in a thread.
- `GET /` returns the page.
- `GET /api/status` returns JSON:
  - `agents`, including each one's current activity when cheap
  - `handoffs`: the last 200, with senderName, recipientName, title and needsAttention
  - `work`: open plus the last 50 closed, with due time and overdue flag
  - `metrics`: counts per state; median and p90 enqueue→ACCEPTED and enqueue→RETURNED over 24 h; resends over 24 h; per-agent open load
  - `health`: the engine health file
  - `generatedAt`
- `GET /api/events?after=N` returns new events.
- Unless `public_demo` is set, only loopback Host headers are served (403 otherwise).
- `POST /api/send` needs header `X-Handoffs-Token` equal to the token in `<db>.token` (created with mode 0600). It is disabled when `public_demo` is set.
- The page is one self-contained HTML file: no external resources, light/dark via prefers-color-scheme, readable at 390 px. It shows:
  - 4 headline tiles: in flight, needs attention, median time to accept, resends in 24 h
  - an agent strip showing each agent's name, provider, state dot and open load
  - live handoff cards (title, from → to, state chip with a plain-English detail, attempts, age), sorted with needs-attention first
  - a work panel with due times
  - an event timeline
  - It polls `/api/status` every 3 s.
- The layout is modern and calm; this is the public face of the project.

### E. MCP server: `src/agentbrain_handoffs/mcp.py`, `tests/test_mcp.py`
A stdio MCP server using newline-delimited JSON-RPC 2.0. It handles `initialize` (echoing the client's protocolVersion if it is one of `2024-11-05`, `2025-03-26` or `2025-06-18`, else the latest), `notifications/initialized`, `tools/list` and `tools/call`.
- `serve_mcp(store, agent_id)`: the identity is fixed at start (`handoffs mcp --agent ID`). A tool can never act as another agent.
- Tools, each with a clear description and JSON schema:
  - `send_handoff(to, message, title?, due_minutes?)`
  - `inbox(unread_only=true)`
  - `read_message(id)`, which also marks it read if you are the recipient
  - `accept_work(id)`
  - `return_work(id, summary, evidence?, blocked?)`
  - `close_work(id, outcome, note?)`
  - `handoff_status(id?)`
  - `list_agents()`
- Errors go back as tool results with `isError: true` and a plain message.

### F. CLI, packaging, docs, Space: `src/agentbrain_handoffs/__init__.py`, `__main__.py`, `cli.py`, `outage.py`, `pyproject.toml`, `README.md`, `docs/*.md` (except SPEC, ENGINE-NOTES), `space/README.md`, `space/Dockerfile`, `.github/workflows/ci.yml`, `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`, `.gitignore`, `tests/test_cli.py`
- `outage.py`: `StatusPageGuard(url, components, incident_keyword=None, ttl=60, fetch=None, clock=time.time)`. It is a callable returning a one-line hold reason while any listed component is in major or full outage and no matching incident is monitoring or resolved. It fails open. Include a preset `openai_codex()`. Store settings `outageGuard: {"preset": "openai-codex"}` or `{"url": ..., "components": [...]}` build it through `outage.from_settings(dict)`.
- CLI `handoffs` (argparse). The DB is `--db`, else env `HANDOFFS_DB`, else `./.handoffs/handoffs.sqlite3`. Commands:
  - `init`
  - `agent add ID --name --provider --endpoint --cwd --set KEY=VALUE…`, `agent list`, `agent remove ID`
  - `allow A B`, `block A B`
  - `config KEY VALUE` (JSON values)
  - `send FROM TO MESSAGE [--title] [--due-minutes] [--key]`
  - `inbox AGENT`
  - `read ID --as AGENT`
  - `return ID --as AGENT --summary TEXT [--evidence] [--blocked]`
  - `close ID --as AGENT accepted|revision|blocked [--note]`
  - `status [--json]`
  - `tick`
  - `run [--interval]` (engine only)
  - `serve [--host --port]` (engine plus web; `--public-demo` is refused here, it is only for `demo`)
  - `release ID [--note]` (cancel one stuck unfinished delivery; nothing is sent)
  - `mcp --agent ID`
  - `demo [--port] [--speed] [--host] [--public-demo]`: a temp DB, 4 demo agents (Planner, Engineer, Reviewer, Writer), a small pipeline of assignments with due times, continued feeding so the page stays alive, then serve. It prints the URL.

  Human output is plain, and there's `--json` where useful. Exit code 2 on usage errors.
- README: this is the project's front door, so make it excellent and honest.
  - One-line pitch, then why (running many agent sessions: lost handoffs, double sends, interrupted agents).
  - A 60-second demo (`pipx run agentbrain-handoffs demo` or `pip install . && handoffs demo`).
  - Concepts, with the states table in plain words; the guarantees; the adapters (demo and command stable, Codex and Claude Code experimental).
  - MCP setup snippets for Claude Code (`claude mcp add handoffs -- handoffs mcp --agent me`), Codex (`~/.codex/config.toml` `[mcp_servers.handoffs]`) and Cursor.
  - CLI reference, a security model section (loopback, token, exact identity) and a roadmap.
  - A final line: "AgentBrain Handoffs is the open delivery layer for AgentBrain, the agent runtime coming to Agent Rooms (agentrooms.io)."
  - Link Hugging Face and GitHub placeholders as `https://github.com/willykeenan/agentbrain-handoffs` and `https://huggingface.co/spaces/willykeenan/agentbrain-handoffs`.
- Space: a Docker Space on port 7860 that runs `handoffs demo --host 0.0.0.0 --port 7860 --public-demo --speed 2`, with `space/README.md` HF front matter (title, emoji 🤝, colorFrom, colorTo, sdk docker, app_port 7860, pinned false, license apache-2.0, short_description).
- CI runs unittest on Python 3.9–3.13 on ubuntu and macos.

## Definition of done (integration)
- `python3 -m unittest discover -s tests` passes on Python 3.9.
- `handoffs demo --port <free>` serves a page where handoffs move through ACCEPTED→RUNNING→RETURNED and work gets returned and closed within 60 s at `--speed 4`. Verify with HTTP polling of `/api/status`.
- No secrets or personal data: a scan for home paths, personal emails, key prefixes and internal tool names finds nothing outside docs that intentionally name the author "KE Studios".
