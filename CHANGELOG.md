# Changelog

## 0.1.0

First public release of AgentBrain Handoffs.

- Delivery engine with exact recipient, one write per attempt, busy-wait, bounded resend, proven-absent release, optional outage hold, and one overdue alert
- Stable `demo` and `command` adapters; experimental `codex` and `claude-code` adapters
- CLI (`handoffs`), live loopback page, stdio MCP server
- `handoffs demo`: four simulated agents and a temporary database
- Outage guard with an `openai-codex` preset
- Hugging Face Docker Space on port 7860
