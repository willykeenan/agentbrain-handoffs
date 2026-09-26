# Security model

AgentBrain Handoffs is a local delivery layer. It stores agent names, session ids, message bodies and work contracts in one SQLite file you choose. It is meant to run next to the agents it serves, on a machine you already trust with those sessions.

## Exact identity

A handoff names one registered recipient. The engine delivers only to that agent, at the endpoint recorded when the row was enrolled. Changing an agent's endpoint leaves queued rows UNAVAILABLE; they are never redirected.

The MCP server's identity is the `--agent` passed at start. Tools have no sender argument. Invented `from` / `as` / `agent_id` fields are refused. Only the assignee can accept or return work; only the original sender can close it.

`--as` on the CLI (or `HANDOFFS_AGENT`) is equally explicit. The tool never guesses who you are.

## The live page is loopback

`handoffs serve` and `handoffs demo` bind to `127.0.0.1` by default. Unless `--public-demo` is set:

- The peer address must be loopback.
- The `Host` header must name loopback. That blocks DNS rebinding, where another origin points a name at 127.0.0.1 to read `/api/status`.

`--public-demo` (used on the Hugging Face Space) serves any Host and turns **off** `POST /api/send`. The public page is read-only. It exists only on `handoffs demo`, whose temporary database holds simulated agents; `handoffs serve` refuses it, and the web server refuses it for any database with a non-demo agent. On the public page each delivery's detail is a plain state sentence, because details can quote adapter errors (paths, hosts, program output).

## Writes from the page need a token

`POST /api/send` requires header `X-Handoffs-Token` equal to the contents of `<db>.token`. The file is created with mode 0600. Another origin cannot read that file, and a cross-site POST cannot attach a custom header without a CORS preflight, which this server does not grant.

Agent settings (commands, URLs, keys), working directories, session endpoints and raw receipts are omitted from `/api/status`.

The page is one HTML file with a strict Content-Security-Policy: no external resources.

## Command adapter

`settings.command` is an argv list, run with no shell. Placeholders are substituted as literal strings. HTTP mode talks to `http`/`https` URLs you configured; loopback URLs skip an HTTP proxy so a local status endpoint stays local.

## Outage guard

`outageGuard` fetches a public Statuspage document. It holds delivery while a named component is in major or full outage, and it **fails open**: a timeout, a malformed body, or an oversized response is treated as "no outage", so a broken status page cannot freeze the team. Answers are cached for `ttl` seconds (default 60). Only `http` and `https` URLs are accepted.

```bash
handoffs config outageGuard '{"preset": "openai-codex"}'
```

## Database file

The SQLite file is created under `.handoffs/` in the working directory, or wherever `--db` / `HANDOFFS_DB` points. It is ordinary local state: protect it the way you protect the repo's secrets. The token file beside it is 0600. The database path must not be a symlink.

Commands other than `init` refuse to create a database, so a mistyped path cannot silently start a second empty world.

## What this package does not do

It does not authenticate agents to each other beyond the ids you registered and the connections you allow or block. It does not encrypt the database. It does not phone home. Codex and Claude Code adapters spawn the binaries you configured; treat those binaries as you already treat those tools.

Report vulnerabilities through GitHub security advisories on the repository. See [SECURITY.md](../SECURITY.md).
