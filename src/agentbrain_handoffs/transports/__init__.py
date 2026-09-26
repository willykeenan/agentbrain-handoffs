"""The adapters that ship with the package, keyed by provider name.

'demo' and 'command' are stable and always load. The Codex and Claude Code
adapters are experimental and are imported only when asked for, each on its own,
so a problem in one of them never stops the others (or the demo) from working.
An adapter that cannot load is replaced by a stand-in that explains why and never
sends anything.
"""
from __future__ import annotations

import importlib

from ..transport import Transport
from .command import CommandTransport
from .demo import DemoTransport

# The experimental classes are left out on purpose: `import *` must not import them.
__all__ = ['available', 'DemoTransport', 'CommandTransport', 'UnavailableTransport']

# provider -> (module inside this package, class name)
EXPERIMENTAL = {
    'codex': ('.codex', 'CodexTransport'),
    'claude-code': ('.claude_code', 'ClaudeCodeTransport'),
}


def available() -> dict:
    """{provider name: adapter class}. Each class can be built with no arguments."""
    adapters = {'demo': DemoTransport, 'command': CommandTransport}
    for provider, (module, class_name) in EXPERIMENTAL.items():
        try:
            adapters[provider] = getattr(importlib.import_module(module, __name__), class_name)
        except Exception as error:  # any failure stays contained to this one provider
            adapters[provider] = unavailable(provider, error)
    return adapters


class UnavailableTransport(Transport):
    """Stands in for an adapter that failed to load. It never sends anything.

    activity() answers 'unknown', so the engine holds deliveries as UNAVAILABLE
    instead of sending blind; prepare() and observe() raise the load error so it
    shows up on the handoff and in the engine health report.
    """

    name = 'unavailable'
    provider = ''
    reason = 'This adapter is not available.'

    def __init__(self, *args, **kwargs):
        pass  # accepts the real adapter's arguments, so callers need no special case

    def activity(self, agent):
        return {'turnStatus': 'unknown', 'turnId': None, 'detail': self.reason}

    def prepare(self, agent):
        raise RuntimeError(self.reason)

    def start(self, prepared, text, request_id):
        raise RuntimeError(self.reason)

    def observe(self, agent, request_id, receipt):
        raise RuntimeError(self.reason)


def unavailable(provider: str, error: BaseException) -> type:
    """A stand-in adapter class for a provider whose module failed to import."""
    reason = ('The ' + provider + ' adapter could not be loaded (' + type(error).__name__ + ': ' +
              str(error)[:200] + ').')
    class_name = ''.join(part.title() for part in provider.split('-')) + 'Unavailable'
    return type(class_name, (UnavailableTransport,), {'provider': provider, 'reason': reason})


def __getattr__(name):
    """`from agentbrain_handoffs.transports import CodexTransport` imports that module only then."""
    for module, class_name in EXPERIMENTAL.values():
        if name == class_name:
            return getattr(importlib.import_module(module, __name__), class_name)
    raise AttributeError('module ' + repr(__name__) + ' has no attribute ' + repr(name))
