"""The live web page and the small JSON API behind it.

`serve()` puts one self-contained page (page.html) and a few endpoints on a
standard-library HTTP server, and can run the delivery engine beside it:

    GET  /              the page
    GET  /api/status    everything the page shows, in one JSON document
    GET  /api/events    the event timeline (?after=N for new events only)
    POST /api/send      put a message in an agent's inbox (token required)
    GET  /healthz       a tiny liveness answer for containers

The API speaks in plain words so the page stays simple: every handoff arrives
with names, a title, a short state label and whether a person should look at it.

Security model, most important first:
- Loopback only. Unless `public_demo` is set, a request must come from a loopback
  address *and* name a loopback Host. The Host check defeats DNS rebinding, where
  a web page on another site points its own name at 127.0.0.1 to read this API.
- Writing needs a secret. POST /api/send requires the X-Handoffs-Token header to
  equal the token in `<db>.token`, a random value created with mode 0600. Another
  site's page can neither read that file nor send a custom header without a CORS
  preflight, which this server never grants. The public demo cannot write at all.
- Nothing private leaves. Agent settings (commands, URLs, keys), working
  directories, session endpoints and raw delivery receipts are never sent.
- The public demo is for simulated data only. `public_demo` serves every Host with
  no login, so `make_server` refuses it unless every registered agent is a `demo`
  agent (the CLI allows it only on `handoffs demo`), and delivery details, which
  can quote adapter errors with local paths, are replaced by plain state sentences.
- The page runs under a strict Content-Security-Policy: no external resources,
  and only the exact inline script and style that ship in page.html (by hash).
"""
from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import secrets
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .store import ACTIVE, PENDING, STATES, TERMINAL

try:
    from . import __version__ as VERSION
except ImportError:  # pragma: no cover - only when the package is half-installed
    VERSION = ''

__all__ = ['Dashboard', 'ensure_token', 'make_server', 'percentile', 'serve', 'token_path']

PAGE_PATH = Path(__file__).with_name('page.html')

HANDOFF_LIMIT = 200       # handoff cards in one status document
CLOSED_WORK_LIMIT = 50    # recently closed work shown beside the open work
OPEN_WORK_LIMIT = 500     # a safety cap; open work is normally a handful
EVENT_TAIL = 50           # events returned when the page first loads
EVENT_MAX = 500           # most events in one response
DAY = 86400
MAX_BODY = 1 << 20        # 1 MiB: a message body is at most 200,000 characters

# Short state names for people. The engine's `detail` sentence says the rest.
LABELS = {
    'WAITING': 'Queued',
    'BUSY': 'Waiting its turn',
    'HELD': 'Held',
    'UNAVAILABLE': 'Unavailable',
    'OWNER_REJECTED': 'Retrying once',
    'SENDING': 'Sending',
    'UNCERTAIN': 'Unconfirmed',
    'ACCEPTED': 'Accepted',
    'RUNNING': 'Working',
    'RETURNED': 'Finished',
    'FAILED': 'Failed',
    'ACKNOWLEDGED': 'Read',
    'CANCELLED': 'Released',
    'DUPLICATE': 'Duplicate',
}

IN_TURN = ('SENDING', 'ACCEPTED', 'RUNNING')           # the recipient has (or is getting) the turn
ALWAYS_ATTENTION = ('UNCERTAIN', 'UNAVAILABLE', 'HELD')  # a person should look while these last
RECENT_ATTENTION = ('FAILED', 'CANCELLED')             # ...and at these for a day, if the work is still open
AGENT_HAS_TURN = ('ACCEPTED', 'RUNNING', 'RETURNED')   # the first of these marks "time to accept"
FAILED_GRACE = 60     # seconds a fresh failure waits for the engine's resend decision before it alerts
RESEND_EVENT = 'Resending automatically%'  # the engine's detail on the WAITING event of a resend

# What the public demo shows instead of the engine's detail sentence. Details can quote an
# adapter's error text (paths, hosts, program output), so a public page never sends them.
PUBLIC_DETAILS = {
    'WAITING': 'Queued for delivery.',
    'BUSY': 'Waiting for the recipient to finish its current turn.',
    'HELD': 'Held: delivery is paused or this connection is blocked.',
    'UNAVAILABLE': 'The recipient cannot be reached right now; nothing is sent blind.',
    'OWNER_REJECTED': 'The recipient refused the turn; one retry is allowed.',
    'SENDING': 'Sending one turn to the recipient.',
    'UNCERTAIN': 'The delivery result is unknown; it is checked, never resent.',
    'ACCEPTED': 'The recipient accepted the turn.',
    'RUNNING': 'The recipient is working on it.',
    'RETURNED': 'The turn finished.',
    'FAILED': 'The turn failed or was stopped.',
    'ACKNOWLEDGED': 'The recipient read the message; no extra turn was needed.',
    'CANCELLED': 'Released without a resend.',
    'DUPLICATE': 'Identical to a handoff still in progress; not sent twice.',
}

# Providers whose activity() is a local, in-process read. Others (which may start an
# app server or read large transcripts) are only asked when the adapter opts in with
# a class attribute `cheap_activity = True`.
CHEAP_PROVIDERS = frozenset({'demo', 'command'})
ACTIVITY_TTL = 2.0

# One query shape describes a handoff for both the cards and the timeline:
# the work contract it carries (w) or the work it returns (rw), plus the first
# characters of a plain message for a title.
_DESCRIBE = '''
  LEFT JOIN messages m ON m.id = {id}
  LEFT JOIN work w ON w.message_id = {id}
  LEFT JOIN work rw ON rw.return_message_id = {id}'''

HANDOFFS_SQL = '''
SELECT h.id, h.sender, h.recipient, h.status, h.detail, h.attempts, h.created, h.updated, h.receipt,
       m.read_at, substr(m.body, 1, 300) AS head,
       w.title AS work_title, w.due_seconds, w.created AS work_created, w.accepted AS work_accepted,
       w.returned AS work_returned, w.closed AS work_closed, rw.title AS return_title
FROM handoffs h''' + _DESCRIBE.format(id='h.id') + '''
ORDER BY h.created DESC LIMIT ?'''

EVENTS_SQL = '''
SELECT e.sequence, e.handoff_id, e.at, e.status, e.detail, h.sender, h.recipient,
       substr(m.body, 1, 300) AS head, w.title AS work_title, rw.title AS return_title
FROM events e LEFT JOIN handoffs h ON h.id = e.handoff_id''' + _DESCRIBE.format(id='e.handoff_id')

WORK_SQL = '''
SELECT w.*, m.sender, m.recipient FROM work w JOIN messages m ON m.id = w.message_id'''

# Time from the moment a message was sent (not when the engine noticed it) to the
# first event of a kind, for handoffs whose first such event falls in the window.
LATENCY_SQL = '''
SELECT MIN(e.at) - m.created AS seconds
FROM events e JOIN messages m ON m.id = e.handoff_id
WHERE e.status IN ({marks}) GROUP BY e.handoff_id HAVING MIN(e.at) >= ?'''

# Everything an agent still owes or is still being handed: open work contracts
# and unfinished deliveries. A handoff id is its message id, so UNION counts a
# work assignment that is also mid-delivery once.
OPEN_LOAD_SQL = '''
SELECT recipient, COUNT(*) AS n FROM (
  SELECT m.recipient AS recipient, w.message_id AS id FROM work w JOIN messages m ON m.id = w.message_id
   WHERE w.returned IS NULL AND w.closed IS NULL
  UNION
  SELECT recipient, id FROM handoffs WHERE status NOT IN ({terminal})
) GROUP BY recipient'''


# ---- small pure helpers -----------------------------------------------------------

def percentile(values, q):
    """The q-quantile (0..1) with linear interpolation, or None for no values.

    Linear interpolation (the common spreadsheet and NumPy default) keeps small
    samples honest: the p90 of three deliveries sits between the two slowest.
    """
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None
    pos = (len(xs) - 1) * min(max(q, 0.0), 1.0)
    lo, hi = math.floor(pos), math.ceil(pos)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (pos - lo), 3)


def _summary(values) -> dict:
    return {'medianSeconds': percentile(values, 0.5), 'p90Seconds': percentile(values, 0.9), 'samples': len(values)}


def _first_line(text, cap=80) -> str:
    """A readable one-line title from a message body (for handoffs without a work title)."""
    for line in str(text or '').splitlines():
        line = ' '.join(line.split()).lstrip('#>*-• ').strip()
        if line:
            return line if len(line) <= cap else line[:cap - 1].rstrip() + '…'
    return ''


def _loads(value) -> dict:
    try:
        parsed = json.loads(value or '{}')
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _describe(row) -> tuple:
    """(title, kind) for a handoff: a work assignment, a returned result, or a plain message."""
    if row['work_title']:
        return row['work_title'], 'assignment'
    if row['return_title']:
        return row['return_title'], 'return'
    return _first_line(row['head']) or 'Message', 'message'


def _work_state(closed, returned, accepted) -> str:
    if closed:
        return 'closed'
    if returned:
        return 'returned'
    return 'accepted' if accepted else 'open'


def _phase(status) -> str:
    return 'active' if status in ACTIVE else 'pending' if status in PENDING else 'terminal'


def token_path(store) -> Path:
    """Where the write token lives: `<db>.token`, next to the database."""
    return store.path.with_name(store.path.name + '.token')


def ensure_token(store) -> str:
    """Return the write token, creating it (mode 0600, never through a symlink) if needed."""
    path = token_path(store)
    if path.is_symlink():
        raise ValueError('The token file must not be a symlink: ' + path.name)
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        with contextlib.suppress(OSError):
            path.chmod(0o600)
        token = path.read_text().strip()
        if len(token) < 16:
            raise ValueError('The token file ' + path.name + ' is too short; delete it to create a new one')
        return token
    token = secrets.token_urlsafe(32)
    with os.fdopen(fd, 'w') as f:
        f.write(token + '\n')
    return token


def loopback_host(header) -> bool:
    """True when a Host header names this computer: localhost or a loopback IP, any port."""
    host = str(header or '').strip().lower()
    if host.startswith('['):
        name = host[1:host.find(']')] if ']' in host else ''
    elif host.count(':') > 1:
        return False  # a bare IPv6 address is not a valid Host header
    else:
        name = host.split(':', 1)[0]
    name = name.rstrip('.')
    if name == 'localhost':
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def loopback_peer(address) -> bool:
    """True when a client socket address is this computer (IPv4, IPv6 or IPv4-mapped)."""
    try:
        ip = ipaddress.ip_address(str(address).split('%', 1)[0])
    except ValueError:
        return False
    if ip.version == 6 and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return ip.is_loopback


def _csp(page: str, public_demo: bool) -> str:
    """A policy that allows exactly the page's own inline script and style, nothing else."""
    def hashes(tag):
        found = re.findall('<' + tag + '>(.*?)</' + tag + '>', page, re.S)
        return ' '.join("'sha256-" + base64.b64encode(hashlib.sha256(body.encode()).digest()).decode() + "'"
                        for body in found) or "'none'"
    policy = ("default-src 'none'; script-src " + hashes('script') + '; style-src ' + hashes('style') +
              "; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'")
    # A hosted demo is shown inside its host's frame; a local page never needs framing.
    return policy if public_demo else policy + "; frame-ancestors 'none'"


# ---- the data behind the page -----------------------------------------------------

class Dashboard:
    """Builds the documents the page shows. Read-only apart from `send`, safe across threads.

    store: the Store to read.
    engine: optional Engine; supplies the health file location and, for cheap
        adapters, each agent's live activity.
    public_demo: marks the payload so the page can say it is a demo.
    clock: seconds since the epoch (defaults to the store's clock, so tests agree).
    cache_seconds: how long one status document is reused. Many viewers polling a
        public demo then cost one database read per second, not one per viewer.
    """

    def __init__(self, store, engine=None, public_demo=False, clock=None, cache_seconds=1.0):
        self.store = store
        self.engine = engine
        self.public_demo = bool(public_demo)
        self.clock = clock or store.clock
        self.cache_seconds = cache_seconds
        if engine is not None and getattr(engine, 'health_path', None):
            self.health_path = Path(engine.health_path)
        else:
            self.health_path = store.path.with_name(store.path.name + '.health.json')
        self._lock = threading.Lock()
        self._cached = (0.0, None)
        self._activity = {}

    # ---- /api/status -------------------------------------------------------------
    def status(self) -> dict:
        with self._lock:
            expires, payload = self._cached
            if payload is None or time.monotonic() >= expires:
                payload = self._build()
                self._cached = (time.monotonic() + self.cache_seconds, payload)
            return payload

    def invalidate(self):
        with self._lock:
            self._cached = (0.0, None)

    def _build(self) -> dict:
        agents = self.store.agents()
        activity = {a['id']: self._activity_of(a) for a in agents}  # adapter calls happen outside any DB read
        names = {a['id']: a['name'] for a in agents}
        now = self.clock()
        terminal = ','.join('?' * len(TERMINAL))
        with self.store.connect() as db:
            # Python 3.12+ starts a transaction on `with db`; 3.9 does not.
            if not db.in_transaction:
                db.execute('BEGIN')  # one consistent snapshot for the whole document
            handoff_rows = db.execute(HANDOFFS_SQL, (HANDOFF_LIMIT,)).fetchall()
            open_work = db.execute(WORK_SQL + ' WHERE w.closed IS NULL ORDER BY w.created DESC LIMIT ?',
                                   (OPEN_WORK_LIMIT,)).fetchall()
            closed_work = db.execute(WORK_SQL + ' WHERE w.closed IS NOT NULL ORDER BY w.closed DESC LIMIT ?',
                                     (CLOSED_WORK_LIMIT,)).fetchall()
            counts = dict.fromkeys(STATES, 0)
            counts.update({r['status']: r['n'] for r in db.execute('SELECT status, COUNT(*) AS n FROM handoffs GROUP BY status')})
            per_state = db.execute('SELECT recipient, status, COUNT(*) AS n FROM handoffs WHERE status NOT IN (' + terminal +
                                   ') GROUP BY recipient, status', TERMINAL).fetchall()
            load = {r['recipient']: r['n'] for r in db.execute(OPEN_LOAD_SQL.format(terminal=terminal), TERMINAL)}
            to_accepted = self._latencies(db, AGENT_HAS_TURN, now - DAY)
            to_returned = self._latencies(db, ('RETURNED',), now - DAY)
            resends = db.execute("SELECT COUNT(*) FROM events WHERE status='WAITING' AND detail LIKE ? AND at>=?",
                                 (RESEND_EVENT, now - DAY)).fetchone()[0]
            last_sequence = db.execute('SELECT COALESCE(MAX(sequence), 0) FROM events').fetchone()[0]

        name = self._namer(names)
        handoffs = [self._handoff(r, name, now) for r in handoff_rows]
        work = [self._work(r, name, now) for r in list(open_work) + list(closed_work)]
        attention_ids = {h['id'] for h in handoffs if h['needsAttention']} | {w['id'] for w in work if w['overdue']}

        in_turn, queued, attention = {}, {}, {}
        for r in per_state:
            bucket = in_turn if r['status'] in IN_TURN else queued if r['status'] in PENDING else None
            if bucket is not None:
                bucket[r['recipient']] = bucket.get(r['recipient'], 0) + r['n']
        for h in handoffs:
            if h['needsAttention']:
                attention[h['recipient']] = attention.get(h['recipient'], 0) + 1

        agent_list = []
        for a in agents:
            aid = a['id']
            agent_list.append({
                'id': aid, 'name': a['name'], 'provider': a['provider'], 'activity': activity[aid],
                'state': self._agent_state(activity[aid], in_turn.get(aid, 0), attention.get(aid, 0), queued.get(aid, 0)),
                'openLoad': load.get(aid, 0), 'inTurn': in_turn.get(aid, 0), 'queued': queued.get(aid, 0),
                'attention': attention.get(aid, 0),
            })

        return {
            'generatedAt': now,
            'version': VERSION,
            'publicDemo': self.public_demo,
            'agents': agent_list,
            'handoffs': handoffs,
            'work': work,
            'metrics': {
                'counts': counts,
                'inFlight': sum(counts[s] for s in ACTIVE),
                'queued': sum(counts[s] for s in PENDING),
                'needsAttention': len(attention_ids),
                'toAccepted': _summary(to_accepted),
                'toReturned': _summary(to_returned),
                'resends24h': resends,
                'openLoad': {a['id']: load.get(a['id'], 0) for a in agents},
            },
            'health': self._health(now),
            'lastSequence': last_sequence,
        }

    @staticmethod
    def _latencies(db, marks, since) -> list:
        sql = LATENCY_SQL.format(marks=','.join('?' * len(marks)))
        return [max(0.0, r['seconds']) for r in db.execute(sql, (*marks, since)) if r['seconds'] is not None]

    def _detail(self, status, detail) -> str:
        """The engine's sentence for this computer's own page; a fixed one on the public demo."""
        if self.public_demo:
            return PUBLIC_DETAILS.get(status, 'Delivery in progress.')
        return detail

    def _namer(self, names):
        def name(agent_id):
            if agent_id in names:
                return names[agent_id]
            return self.store.name(agent_id) if agent_id else 'Unknown agent'
        return name

    def _handoff(self, row, name, now) -> dict:
        title, kind = _describe(row)
        receipt = _loads(row['receipt'])
        status = row['status']
        work_state = due_at = None
        if kind == 'assignment':
            work_state = _work_state(row['work_closed'], row['work_returned'], row['work_accepted'])
            if row['due_seconds']:
                due_at = row['work_created'] + row['due_seconds']
        overdue = bool(due_at is not None and work_state in ('open', 'accepted') and due_at <= now)
        # Still open means someone still expects this delivery to matter: unreturned
        # work, or a plain message (or returned result) that nobody has read.
        still_open = work_state in ('open', 'accepted') if kind == 'assignment' else not row['read_at']
        recent_failure = (status in RECENT_ATTENTION and still_open and now - row['updated'] < DAY and
                          (status == 'CANCELLED' or receipt.get('resendDecision') == 'final'
                           or now - row['updated'] >= FAILED_GRACE))
        return {
            'id': row['id'], 'kind': kind, 'title': title,
            'sender': row['sender'], 'recipient': row['recipient'],
            'senderName': name(row['sender']), 'recipientName': name(row['recipient']),
            'status': status, 'label': LABELS.get(status, status.title()), 'phase': _phase(status),
            'detail': self._detail(status, row['detail']), 'attempts': row['attempts'],
            'resends': int(receipt.get('resends') or 0),
            'created': row['created'], 'updated': row['updated'],
            'workState': work_state, 'dueAt': due_at, 'overdue': overdue,
            'needsAttention': status in ALWAYS_ATTENTION or overdue or recent_failure,
        }

    @staticmethod
    def _work(row, name, now) -> dict:
        result, closure = _loads(row['result']), _loads(row['closure'])
        state = _work_state(row['closed'], row['returned'], row['accepted'])
        due_at = row['created'] + row['due_seconds'] if row['due_seconds'] else None
        return {
            'id': row['message_id'], 'title': row['title'],
            'sender': row['sender'], 'recipient': row['recipient'],
            'senderName': name(row['sender']), 'recipientName': name(row['recipient']),
            'state': state, 'created': row['created'], 'accepted': row['accepted'],
            'returned': row['returned'], 'closed': row['closed'],
            'dueAt': due_at, 'overdue': bool(due_at is not None and state in ('open', 'accepted') and due_at <= now),
            'summary': result.get('summary'), 'disposition': result.get('disposition'),
            'outcome': closure.get('outcome'), 'note': closure.get('note') or None,
        }

    @staticmethod
    def _agent_state(activity, in_turn, attention, queued) -> str:
        """working > attention > queued > idle. A known activity beats the delivery rows,
        which can lag the agent app by one engine pass."""
        if activity == 'open' or (activity in (None, 'unknown') and in_turn):
            return 'working'
        if attention:
            return 'attention'
        return 'queued' if queued else 'idle'

    def _activity_of(self, agent):
        """The agent's turn status when asking is cheap, else None. Cached for two seconds."""
        if self.engine is None or not self._cheap(agent):
            return None
        now = time.monotonic()
        hit = self._activity.get(agent['id'])
        if hit and hit[0] > now:
            return hit[1]
        try:
            value = self.engine.activity(agent).get('turnStatus', 'unknown')
        except Exception:  # noqa: BLE001 - the page must render even if an adapter breaks
            value = 'unknown'
        self._activity[agent['id']] = (now + ACTIVITY_TTL, value)
        return value

    def _cheap(self, agent) -> bool:
        transport = getattr(self.engine, 'transport', None)
        with contextlib.suppress(Exception):
            transport = transport.adapter(agent)  # a Router: ask the adapter behind this provider
        flag = getattr(transport, 'cheap_activity', None)
        if flag is not None:
            return bool(flag)
        provider = agent.get('provider')
        if provider == 'command':
            settings = agent.get('settings')
            url = settings.get('url') if isinstance(settings, dict) else None
            return not url  # a status_url GET is a network round-trip, not a local read
        return provider in CHEAP_PROVIDERS

    def _health(self, now):
        """The engine's last pass, trimmed to plain facts (error text can hold local paths)."""
        try:
            report = json.loads(self.health_path.read_text())
        except (OSError, ValueError):
            return None
        if not isinstance(report, dict):
            return None
        at = report.get('at') if isinstance(report.get('at'), (int, float)) else None
        errors = report.get('errors')
        return {
            'at': at, 'ok': bool(report.get('ok')),
            'ageSeconds': round(now - at, 1) if at is not None else None,
            'checked': report.get('checked'), 'enrolled': report.get('enrolled'),
            'resends': report.get('resends'), 'overdueAlerts': report.get('overdueAlerts'),
            'errors': len(errors) if isinstance(errors, list) else 0,
            'elapsedSeconds': report.get('elapsedSeconds'),
        }

    # ---- /api/events -------------------------------------------------------------
    def events(self, after=None, limit=None) -> dict:
        """New events after a sequence number, or (without `after`) the latest few."""
        with self.store.connect() as db:
            if not db.in_transaction:
                db.execute('BEGIN')
            if after is None:
                limit = min(limit or EVENT_TAIL, EVENT_MAX)
                rows = db.execute(EVENTS_SQL + ' ORDER BY e.sequence DESC LIMIT ?', (limit,)).fetchall()[::-1]
                last = db.execute('SELECT COALESCE(MAX(sequence), 0) FROM events').fetchone()[0]
            else:
                limit = min(limit or EVENT_MAX, EVENT_MAX)
                rows = db.execute(EVENTS_SQL + ' WHERE e.sequence > ? ORDER BY e.sequence LIMIT ?', (after, limit)).fetchall()
                last = after
        name = self._namer({a['id']: a['name'] for a in self.store.agents()})
        events = []
        for r in rows:
            title, kind = _describe(r)
            events.append({
                'sequence': r['sequence'], 'handoffId': r['handoff_id'], 'at': r['at'],
                'status': r['status'], 'label': LABELS.get(r['status'], r['status'].title()),
                'phase': _phase(r['status']), 'detail': self._detail(r['status'], r['detail']), 'title': title,
                'kind': kind,
                'senderName': name(r['sender']), 'recipientName': name(r['recipient']),
            })
        return {'events': events, 'last': events[-1]['sequence'] if events else last,
                'more': len(events) == limit and after is not None}

    # ---- POST /api/send ----------------------------------------------------------
    def send(self, payload) -> dict:
        """Validate a JSON send request and hand it to Store.send (which checks the rest)."""
        if not isinstance(payload, dict):
            raise ValueError('Send a JSON object with "from", "to" and "message".')
        for field in ('from', 'to', 'message'):
            if not isinstance(payload.get(field), str) or not payload[field].strip():
                raise ValueError('"' + field + '" is required and must be text.')
        for field in ('title', 'key'):
            if payload.get(field) is not None and not isinstance(payload[field], str):
                raise ValueError('"' + field + '" must be text.')
        due = payload.get('due_minutes')
        due_seconds = None
        if due is not None:
            if isinstance(due, bool) or not isinstance(due, (int, float)) or not math.isfinite(due):
                raise ValueError('"due_minutes" must be a number of minutes.')
            due_seconds = int(round(due * 60))
        mid = self.store.send(payload['from'], payload['to'], payload['message'], key=payload.get('key'),
                              title=payload.get('title'), due_seconds=due_seconds)
        self.invalidate()
        return {'id': mid, 'work': payload.get('title') is not None}


# ---- HTTP ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    """Routes requests to the Dashboard. One instance per request (stdlib design)."""

    server_version = 'AgentBrainHandoffs/' + (VERSION or '0')
    sys_version = ''
    timeout = 30  # a stalled client never holds a worker thread for long

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        """Quiet by default: the page polls every few seconds and would flood the terminal."""

    @property
    def app(self):
        return self.server.app

    # ---- plumbing ----------------------------------------------------------------
    def _send(self, code, body: bytes, content_type, headers=None):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Connection', 'close')
        self.close_connection = True
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != 'HEAD':
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

    def _json(self, code, payload):
        self._send(code, json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode(),
                   'application/json; charset=utf-8')

    def _error(self, code, message):
        self._json(code, {'error': message})

    def _allowed(self) -> bool:
        if self.app['public_demo']:
            return True
        if loopback_peer(self.client_address[0]) and loopback_host(self.headers.get('Host')):
            return True
        port = self.server.server_address[1]
        self._send(403, ('This server only answers requests addressed to this computer. '
                         'Open http://127.0.0.1:' + str(port) + '/ instead.\n').encode(), 'text/plain; charset=utf-8')
        return False

    def _route(self, routes):
        if not self._allowed():
            return
        url = urlsplit(self.path)
        handler = routes.get(url.path)
        if handler is None:
            return self._error(404, 'Not found.')
        try:
            handler(parse_qs(url.query))
        except Exception:  # noqa: BLE001 - never leak a traceback to a browser
            self._error(500, 'The server hit an unexpected error. Details are not shown here for safety.')

    # ---- methods -----------------------------------------------------------------
    def do_GET(self):
        self._route({'/': self._page, '/index.html': self._page, '/api/status': self._status,
                     '/api/events': self._events, '/healthz': self._healthz})

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        self._route({'/api/send': self._post_send})

    def _not_allowed(self):
        if self._allowed():
            self._error(405, 'Method not allowed.')

    do_PUT = do_PATCH = do_DELETE = do_OPTIONS = _not_allowed

    # ---- endpoints ---------------------------------------------------------------
    def _page(self, query):
        headers = {'Content-Security-Policy': self.app['csp']}
        if not self.app['public_demo']:
            headers['X-Frame-Options'] = 'DENY'
        self._send(200, self.app['page'], 'text/html; charset=utf-8', headers)

    def _status(self, query):
        self._json(200, self.app['dashboard'].status())

    def _healthz(self, query):
        self._json(200, {'ok': True})

    def _events(self, query):
        try:
            after = int(query['after'][0]) if 'after' in query else None
            limit = int(query['limit'][0]) if 'limit' in query else None
            if (after is not None and after < 0) or (limit is not None and limit < 1):
                raise ValueError
        except ValueError:
            return self._error(400, '"after" and "limit" must be whole numbers ("after" from 0, "limit" from 1).')
        self._json(200, self.app['dashboard'].events(after, limit))

    def _post_send(self, query):
        if self.app['public_demo']:
            return self._error(403, 'Sending is turned off on the public demo.')
        store = self.app['dashboard'].store
        given = (self.headers.get('X-Handoffs-Token') or '').strip()
        if not given:
            return self._error(401, 'Add the X-Handoffs-Token header. The token is in ' + token_path(store).name +
                               ', next to the database.')
        # Hash both sides so a different-length guess cannot raise (compare_digest
        # requires equal length) and cannot leak the stored token's size.
        try:
            match = hmac.compare_digest(hashlib.sha256(given.encode('utf-8')).digest(),
                                        hashlib.sha256(ensure_token(store).encode('utf-8')).digest())
        except (TypeError, ValueError):
            match = False
        if not match:
            return self._error(403, 'The X-Handoffs-Token header does not match the token file.')
        ctype = (self.headers.get('Content-Type') or '').split(';', 1)[0].strip().lower()
        if ctype != 'application/json':
            return self._error(415, 'Send JSON with Content-Type: application/json.')
        try:
            length = int(self.headers.get('Content-Length') or '')
        except ValueError:
            return self._error(411, 'A Content-Length header is required.')
        if length < 0 or length > MAX_BODY:
            return self._error(413, 'The request is larger than 1 MiB.')
        try:
            payload = json.loads(self.rfile.read(length).decode('utf-8'))
        except (UnicodeDecodeError, ValueError):
            return self._error(400, 'The request body is not valid JSON.')
        try:
            result = self.app['dashboard'].send(payload)
        except ValueError as e:
            return self._error(400, str(e))
        self._json(201, result)


class _Server(ThreadingHTTPServer):
    daemon_threads = True      # a slow client never blocks shutdown
    allow_reuse_address = True


class _Server6(_Server):
    address_family = socket.AF_INET6


def make_server(store, engine=None, host='127.0.0.1', port=8765, public_demo=False, cache_seconds=1.0):
    """Build (but do not start) the HTTP server. Port 0 picks a free port.

    The returned server has `.url` (where to open the page) and `.dashboard`.
    Call `serve_forever()` to run it and `shutdown()` from another thread to stop.
    """
    page = PAGE_PATH.read_text(encoding='utf-8')
    if public_demo:
        real = sorted(a['id'] for a in store.agents() if a.get('provider') != 'demo')
        if real:
            raise ValueError('The public demo serves anyone with no login, so it only shows simulated demo agents; '
                             'this database has real ones (' + ', '.join(real[:5]) + '). Serve it without public_demo.')
    else:
        ensure_token(store)  # create it now, so the owner can find it before the first send
    server_class = _Server6 if ':' in host else _Server
    server = server_class((host, port), _Handler)
    dashboard = Dashboard(store, engine=engine, public_demo=public_demo, cache_seconds=cache_seconds)
    server.app = {'dashboard': dashboard, 'public_demo': bool(public_demo),
                  'page': page.encode('utf-8'), 'csp': _csp(page, bool(public_demo))}
    server.dashboard = dashboard
    bound = server.server_address[1]
    shown = {'': '127.0.0.1', '0.0.0.0': '127.0.0.1', '::': '::1'}.get(host, host)
    server.url = 'http://' + ('[' + shown + ']' if ':' in shown else shown) + ':' + str(bound) + '/'
    return server


def serve(store, engine=None, host='127.0.0.1', port=8765, public_demo=False, *, interval=2.0, ready=None):
    """Serve the page until interrupted. With an engine, deliveries run in a background thread.

    interval: seconds between engine passes.
    ready: optional callback, called with the running server (it has `.url`); a
        caller that needs to stop the server calls `server.shutdown()` from it or later.
    """
    server = make_server(store, engine=engine, host=host, port=port, public_demo=public_demo)
    stop = threading.Event()
    worker = None
    if engine is not None:
        worker = threading.Thread(target=engine.run_forever, kwargs={'interval': interval, 'stop': stop.is_set},
                                  name='handoffs-engine', daemon=True)
        worker.start()
    try:
        if ready is not None:
            ready(server)
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        if worker is not None:
            worker.join(timeout=interval + 5)
