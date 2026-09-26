# Contributing

Thanks for looking. AgentBrain Handoffs is a small Python package with a strict contract: exact delivery, one write per attempt, no interrupting a busy agent, bounded resend. Changes should keep those guarantees and the tests that prove them.

## Setup

Python 3.9+ on macOS or Linux.

```bash
python3 -m venv .venv
.venv/bin/pip install .
.venv/bin/python -m unittest discover -s tests -v
```

There are no third-party runtime dependencies. Do not add any.

## Tests

`python -m unittest discover -s tests` is the suite. CI runs it on Python 3.9–3.13 on Ubuntu and macOS.

Rules the tests already enforce:

- Stay offline. No network except `127.0.0.1` in a test you start yourself.
- Never call a real `codex` or `claude` binary. Adapter tests use fake executables that speak the protocol.
- Engine tests prove the guarantees in `engine.py`'s docstring. If you change delivery behaviour, extend those tests.

A live demo check exists in `tests/test_cli.py` and is skipped unless `HANDOFFS_E2E=1`.

## Code

- Target Python 3.9. Standard library only.
- Public behaviour lives behind `Store`, `Engine` and `Transport`. The CLI, the MCP server and the web page should stay thin.
- Import `web`, `mcp` and the experimental adapters only inside the command or factory that needs them, so a problem in one does not break the others.
- Human-facing errors start with `handoffs: ` and say what to do next. Usage errors exit 2.
- No private paths, emails, tokens, or machine-specific names in the tree, and none spelled in pieces to dodge a search. `tests/test_web.py` scans every shipped file; set `HANDOFFS_PRIVATE_PATTERNS` (comma-separated) locally or in CI to add names that must never appear.

## Publishing

A push publishes the whole history, not just the tree. Before adding a remote, run `python3 tools/release_check.py REF` with `HANDOFFS_PRIVATE_PATTERNS` set; it fails on a non-noreply author or committer email and on a private pattern in any commit message or any version of any file. Publish only a ref that passes (for example a fresh orphan branch), and push only that ref.

## Docs and the demo

The README is the front door. If you change a command, a flag, or a guarantee, update README.md and the matching file under `docs/`. Run `handoffs demo` and confirm the 60-second quickstart still matches what you printed.

## Pull requests

Small, one concern per PR. Include tests. Mention any guarantee you touched.
