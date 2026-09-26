"""The adapter contract between the delivery engine and an agent app.

An adapter delivers one handoff as one new turn in an *existing* agent session and
later reports what happened to that exact turn. The engine owns every decision
(who, when, whether to resend); adapters only perform and observe.

Rules every adapter must keep:
- activity() never starts anything. Unknown activity returns {'turnStatus': 'unknown'}.
- start() makes at most one provider write. It returns a receipt with 'turnId' and
  'clientUserMessageId' equal to the request id when the provider confirmed the turn.
  Anything less makes the delivery UNCERTAIN, which is reconciled but never resent.
- Raise NotAccepted only when the provider explicitly confirmed it did NOT accept the
  turn (never on a timeout or unknown reply). That is the one case a retry is safe.
- observe() is read-only and matches the turn by its receipt or by the unique
  'HANDOFF <request id>' marker in a real user message, never by quoted text.
"""
from __future__ import annotations

import contextlib

TURN_STATUSES = ('open', 'finished', 'stopped', 'failed', 'unknown')
OBSERVED = ('ACCEPTED', 'RUNNING', 'RETURNED', 'FAILED')


class NotAccepted(RuntimeError):
    """The provider explicitly rejected the turn before accepting it. Safe to retry once."""


class Transport:
    name = 'base'

    def activity(self, agent: dict) -> dict:
        """{'turnStatus': one of TURN_STATUSES, 'turnId': str|None}. Read-only."""
        return {'turnStatus': 'unknown', 'turnId': None}

    @contextlib.contextmanager
    def prepare(self, agent: dict):
        """Validate the exact target and yield whatever start() needs. No provider write."""
        yield {'agent': agent}

    def start(self, prepared: dict, text: str, request_id: str) -> dict:
        """One provider write. Return {'turnId': ..., 'clientUserMessageId': request_id, ...}."""
        raise NotImplementedError

    def observe(self, agent: dict, request_id: str, receipt: dict) -> dict:
        """{'status': one of OBSERVED or None, 'detail': str, 'receipt': dict}. Read-only.

        When a complete search found no turn for this request at all, include
        receipt['historySearch'] = {'exhausted': True, 'candidate': None, 'turnId': None}.
        """
        return {}


class Router(Transport):
    """Dispatches by the agent's provider name to the adapter registered for it."""
    name = 'router'

    def __init__(self, adapters: dict | None = None):
        self.adapters = dict(adapters or {})

    def register(self, provider: str, adapter: Transport):
        self.adapters[provider] = adapter

    def adapter(self, agent: dict) -> Transport:
        adapter = self.adapters.get(agent.get('provider'))
        if adapter is None:
            raise ValueError('No adapter for provider ' + str(agent.get('provider')))
        return adapter

    def activity(self, agent):
        try:
            return self.adapter(agent).activity(agent)
        except ValueError:
            return {'turnStatus': 'unknown', 'turnId': None}

    @contextlib.contextmanager
    def prepare(self, agent):
        with self.adapter(agent).prepare(agent) as prepared:
            yield {'agent': agent, 'inner': prepared}

    def start(self, prepared, text, request_id):
        return self.adapter(prepared['agent']).start(prepared['inner'], text, request_id)

    def observe(self, agent, request_id, receipt):
        return self.adapter(agent).observe(agent, request_id, receipt)


def default_router() -> Router:
    """Every adapter that ships with the package, keyed by provider name."""
    router = Router()
    from .transports import available
    for provider, factory in available().items():
        router.register(provider, factory())
    return router
