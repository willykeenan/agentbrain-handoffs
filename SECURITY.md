# Security policy

AgentBrain Handoffs is local software: a SQLite file, a loopback web page, and subprocesses you configure. Please report vulnerabilities privately.

## Report

Use GitHub security advisories on [willykeenan/agentbrain-handoffs](https://github.com/willykeenan/agentbrain-handoffs). Include the version (`handoffs --version`), the command you ran, and a repro that does not need secrets.

We aim to acknowledge a report within a week and to ship a fix before any public write-up.

## In scope

- Delivery to the wrong agent or endpoint
- A second send of a handoff that should have been unique
- Reading or writing the live page from a non-loopback origin without `--public-demo`
- `POST /api/send` succeeding without the token in `<db>.token`
- An MCP tool acting as an agent other than `--agent`
- Command adapter shell injection through placeholders or settings
- Path traversal or symlink tricks on the database file
- Secrets (tokens, agent settings, receipts) appearing in `/api/status` or the page

## Out of scope

- Anyone with filesystem access to the database reading its contents (it is local state, unencrypted by design)
- Behaviour of `codex` or `claude` themselves
- Denial of service against a process you already control
- Social tricks that make you `allow` a pair or paste a token

## Model

See [docs/SECURITY-MODEL.md](docs/SECURITY-MODEL.md) for loopback, tokens, exact identity and fail-open outage handling.
