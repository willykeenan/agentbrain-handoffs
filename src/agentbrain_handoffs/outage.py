"""Hold deliveries while an agent provider reports an outage, without spending attempts.

When a provider is down, sending a handoff would fail and burn one of its bounded
attempts. The engine asks an optional guard before every send; a non-empty answer
holds the row in place (no attempt used) and delivery resumes by itself once the
provider recovers.

The guard reads a public status page in the common "Statuspage" JSON shape
(``/api/v2/summary.json``): ``components`` with a ``status`` and ``incidents``
with a ``status``. It holds only while a listed component is in a major or full
outage *and* no matching incident has reached monitoring or resolved. Anything it
cannot read or understand counts as "no outage": the guard fails open, because a
broken status page must never stop delivery on its own.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request

DOWN = ('major_outage', 'full_outage')
RECOVERING = ('monitoring', 'resolved', 'postmortem')
MAX_RESPONSE_BYTES = 1_000_000
FETCH_TIMEOUT = 5

OPENAI_STATUS_URL = 'https://status.openai.com/api/v2/summary.json'
# The components that carry desktop and command-line Codex traffic.
OPENAI_CODEX_COMPONENTS = ('Codex API', 'CLI', 'VS Code extension')


def _normal(status) -> str:
    """'Major Outage', 'major-outage' and 'major_outage' all mean the same thing."""
    return '_'.join(str(status or '').strip().lower().replace('-', ' ').split())


def fetch_json(url, timeout=FETCH_TIMEOUT):
    """Download and parse one status document, refusing oversized answers."""
    request = urllib.request.Request(url, headers={'Accept': 'application/json',
                                                   'User-Agent': 'agentbrain-handoffs outage guard'})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 (scheme checked in __init__)
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError('Status document is too large')
    return json.loads(raw)


class StatusPageGuard:
    """A callable that returns a one-line hold reason during an outage, else None.

    Answers are cached for ``ttl`` seconds (failures too) so a slow or unreachable
    status page costs at most one request per minute, not one per handoff.
    """

    def __init__(self, url, components, incident_keyword=None, ttl=60, fetch=None, clock=time.time):
        if urllib.parse.urlsplit(str(url)).scheme not in ('http', 'https'):
            raise ValueError('The status page URL must start with http:// or https://')
        names = [components] if isinstance(components, str) else list(components or [])
        if not names or not all(isinstance(n, str) and n.strip() for n in names):
            raise ValueError('List at least one status page component name to watch')
        self.url = str(url)
        self.components = tuple(n.strip() for n in names)
        self.incident_keyword = (incident_keyword or '').strip().lower() or None
        self.ttl = float(ttl)
        self.fetch = fetch or fetch_json
        self.clock = clock
        self._checked_at = None
        self._answer = None

    def __call__(self):
        at = self.clock()
        if self._checked_at is not None and at - self._checked_at < self.ttl:
            return self._answer
        try:
            answer = self.evaluate(self.fetch(self.url))
        except Exception:
            answer = None  # fail open: an unreadable status page never holds delivery
        self._checked_at, self._answer = at, answer
        return answer

    def evaluate(self, summary):
        """Pure decision over one parsed status document. Raises on a malformed one."""
        watched = {name.lower() for name in self.components}
        down = [c['name'] for c in summary.get('components') or []
                if isinstance(c, dict) and str(c.get('name', '')).lower() in watched and _normal(c.get('status')) in DOWN]
        if not down:
            return None
        for incident in summary.get('incidents') or []:
            if isinstance(incident, dict) and self._matches(incident, watched) and _normal(incident.get('status')) in RECOVERING:
                return None  # the provider says it is recovering; let delivery try again
        return ('Held: the provider status page reports an outage (' + ', '.join(down) + '). '
                'No attempt was used; delivery resumes automatically when it recovers.')

    def _matches(self, incident, watched) -> bool:
        if self.incident_keyword and self.incident_keyword in str(incident.get('name', '')).lower():
            return True
        affected = {str(c.get('name', '') if isinstance(c, dict) else c).lower() for c in incident.get('components') or []}
        return bool(affected & watched)


def openai_codex(**options) -> StatusPageGuard:
    """Preset for Codex agents: watches OpenAI's status page for a Codex outage."""
    return StatusPageGuard(OPENAI_STATUS_URL, OPENAI_CODEX_COMPONENTS, incident_keyword='codex', **options)


PRESETS = {'openai-codex': openai_codex}


def from_settings(config, **options):
    """Build a guard from the ``outageGuard`` store setting, or None when it is off.

    Accepted shapes: ``{"preset": "openai-codex"}`` or
    ``{"url": ..., "components": [...], "incident_keyword": ..., "ttl": 60}``.
    ``options`` (fetch, clock) are passed through, which keeps tests offline.
    """
    if not config:
        return None
    if not isinstance(config, dict):
        raise ValueError('outageGuard must be a JSON object, for example {"preset": "openai-codex"}')
    config = dict(config)
    if 'ttl' in config:
        options.setdefault('ttl', config.pop('ttl'))
    if 'preset' in config:
        preset = PRESETS.get(config.pop('preset'))
        if preset is None:
            raise ValueError('Unknown outage guard preset. Choose one of: ' + ', '.join(sorted(PRESETS)))
        if config:
            raise ValueError('A preset takes no other keys except ttl: ' + ', '.join(sorted(config)))
        return preset(**options)
    unknown = set(config) - {'url', 'components', 'incident_keyword'}
    if unknown or 'url' not in config or 'components' not in config:
        raise ValueError('outageGuard needs "url" and "components" (optional: "incident_keyword", "ttl")')
    return StatusPageGuard(config['url'], config['components'], config.get('incident_keyword'), **options)
