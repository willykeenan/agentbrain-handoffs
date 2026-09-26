---
title: AgentBrain Handoffs
emoji: 🤝
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
license: apache-2.0
short_description: Exact, durable handoffs between AI coding agents
---

# AgentBrain Handoffs

A live demo of [AgentBrain Handoffs](https://github.com/willykeenan/agentbrain-handoffs): four simulated agents (Planner, Engineer, Reviewer, Writer) handing work to each other.

Each handoff is exactly one new turn for the recipient. The page on this Space is the same one `handoffs demo` serves locally. Nothing on the public demo can send a message; it is read-only.

```
handoffs demo --host 0.0.0.0 --port 7860 --public-demo --speed 2
```

Python 3.9+, standard library only. Apache-2.0.
