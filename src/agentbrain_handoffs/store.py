"""Durable local state: agents, directed connections, inbox messages, work contracts,
handoff deliveries and their event history. One SQLite file, stdlib only.

Everything that changes state goes through this module so the delivery engine,
the command line, the MCP server and the web page all see one truth.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import re
import sqlite3
import time
import uuid
from pathlib import Path

SCHEMA_VERSION = 1
AGENT_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9:._@/-]{0,199}$')
INTENTS = ('handoff', 'notification')
CLOSE_OUTCOMES = ('accepted', 'revision', 'blocked')

# Delivery states. PENDING rows may still be delivered; ACTIVE rows had exactly one
# provider write reserved; TERMINAL rows are finished and never touched again.
PENDING = ('WAITING', 'BUSY', 'HELD', 'UNAVAILABLE', 'OWNER_REJECTED')
ACTIVE = ('SENDING', 'UNCERTAIN', 'ACCEPTED', 'RUNNING')
TERMINAL = ('RETURNED', 'FAILED', 'ACKNOWLEDGED', 'CANCELLED', 'DUPLICATE')
STATES = PENDING + ACTIVE + TERMINAL

DEFAULT_SETTINGS = {
    'enabled': True,            # delivery on/off switch
    'enabledAfter': 0.0,        # messages older than this are never delivered (no mass wake)
    'connections': 'open',      # 'open': any registered pair may hand off; 'explicit': only allowed pairs
    'outageGuard': None,        # optional status-page guard config, see outage.py
}

SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS agents(
  id TEXT PRIMARY KEY, name TEXT NOT NULL, provider TEXT NOT NULL, endpoint TEXT NOT NULL DEFAULT '',
  cwd TEXT NOT NULL DEFAULT '', settings TEXT NOT NULL DEFAULT '{}', created REAL NOT NULL, updated REAL NOT NULL);
CREATE TABLE IF NOT EXISTS connections(
  sender TEXT NOT NULL, recipient TEXT NOT NULL, allow INTEGER NOT NULL, updated REAL NOT NULL,
  PRIMARY KEY(sender, recipient));
CREATE TABLE IF NOT EXISTS messages(
  id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipient TEXT NOT NULL, body TEXT NOT NULL,
  intent TEXT NOT NULL DEFAULT 'handoff', key TEXT UNIQUE, created REAL NOT NULL, read_at REAL);
CREATE INDEX IF NOT EXISTS messages_inbox ON messages(recipient, read_at, created);
CREATE TABLE IF NOT EXISTS work(
  message_id TEXT PRIMARY KEY, title TEXT NOT NULL, due_seconds INTEGER, created REAL NOT NULL,
  accepted REAL, returned REAL, closed REAL, result TEXT, closure TEXT, return_message_id TEXT,
  overdue_alerted REAL);
CREATE TABLE IF NOT EXISTS handoffs(
  id TEXT PRIMARY KEY, sender TEXT NOT NULL, recipient TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
  body_sha TEXT NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  created REAL NOT NULL, updated REAL NOT NULL, next_check REAL NOT NULL DEFAULT 0,
  detail TEXT NOT NULL DEFAULT '', receipt TEXT NOT NULL DEFAULT '{}');
CREATE INDEX IF NOT EXISTS handoffs_due ON handoffs(status, next_check);
CREATE TABLE IF NOT EXISTS events(
  sequence INTEGER PRIMARY KEY, handoff_id TEXT NOT NULL, at REAL NOT NULL, status TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
'''


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class Store:
    """All durable state. Safe for several processes (SQLite locking, short transactions)."""

    def __init__(self, path, clock=time.time):
        self.path = Path(path)
        self.clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise ValueError('The handoff database must not be a symlink')
        with self.connect() as db:
            db.executescript(SCHEMA)
            db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', ('schema', str(SCHEMA_VERSION)))
        with contextlib.suppress(OSError):
            self.path.chmod(0o600)

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    # ---- settings -----------------------------------------------------------------
    def settings(self) -> dict:
        with self.connect() as db:
            saved = {r['key']: json.loads(r['value']) for r in db.execute('SELECT key,value FROM settings')}
        return {**DEFAULT_SETTINGS, **saved}

    def configure(self, **values) -> dict:
        unknown = set(values) - set(DEFAULT_SETTINGS)
        if unknown:
            raise ValueError('Unknown setting: ' + ', '.join(sorted(unknown)))
        if 'connections' in values and values['connections'] not in ('open', 'explicit'):
            raise ValueError("connections must be 'open' or 'explicit'")
        with self.connect() as db:
            for key, value in values.items():
                db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)', (key, json.dumps(value)))
        return self.settings()

    # ---- agents -------------------------------------------------------------------
    def register_agent(self, agent_id, name=None, provider='demo', endpoint='', cwd='', settings=None) -> dict:
        if not isinstance(agent_id, str) or not AGENT_ID.fullmatch(agent_id):
            raise ValueError('Agent id: 1-200 characters, letters, digits and :._@/- only')
        name = (name or agent_id).strip()[:120]
        at = self.clock()
        with self.connect() as db:
            db.execute('''INSERT INTO agents(id,name,provider,endpoint,cwd,settings,created,updated) VALUES(?,?,?,?,?,?,?,?)
                          ON CONFLICT(id) DO UPDATE SET name=excluded.name, provider=excluded.provider,
                          endpoint=excluded.endpoint, cwd=excluded.cwd, settings=excluded.settings, updated=excluded.updated''',
                       (agent_id, name, provider, endpoint, cwd, json.dumps(settings or {}), at, at))
        return self.agent(agent_id)

    def agent(self, agent_id):
        with self.connect() as db:
            row = db.execute('SELECT * FROM agents WHERE id=?', (agent_id,)).fetchone()
        return self._agent(row) if row else None

    def agents(self):
        with self.connect() as db:
            return [self._agent(r) for r in db.execute('SELECT * FROM agents ORDER BY created, id')]

    @staticmethod
    def _agent(row):
        a = dict(row)
        a['settings'] = json.loads(a['settings'])
        return a

    def remove_agent(self, agent_id):
        with self.connect() as db:
            db.execute('DELETE FROM agents WHERE id=?', (agent_id,))

    def name(self, agent_id) -> str:
        if agent_id.startswith('service:'):
            return {'service:deadline': 'Deadline service', 'service:router': 'Flow router'}.get(agent_id, 'Service')
        a = self.agent(agent_id)
        return a['name'] if a else 'Unknown agent'

    # ---- connections --------------------------------------------------------------
    def set_connection(self, sender, recipient, allow=True):
        with self.connect() as db:
            db.execute('INSERT OR REPLACE INTO connections VALUES(?,?,?,?)', (sender, recipient, int(bool(allow)), self.clock()))

    def connections(self):
        with self.connect() as db:
            return [dict(r) for r in db.execute('SELECT * FROM connections ORDER BY sender, recipient')]

    def allowed(self, sender, recipient) -> bool:
        """Directed permission. An explicit block always wins; 'explicit' mode needs an allow."""
        if sender == recipient:
            return False
        with self.connect() as db:
            row = db.execute('SELECT allow FROM connections WHERE sender=? AND recipient=?', (sender, recipient)).fetchone()
        if row is not None:
            return bool(row['allow'])
        if sender.startswith('service:'):
            return True  # built-in services only send non-waking notifications or router handoffs
        return self.settings()['connections'] == 'open'

    # ---- messages -----------------------------------------------------------------
    def send(self, sender, recipient, body, intent='handoff', key=None, title=None, due_seconds=None) -> str:
        """Put one message in the recipient's inbox. With a title it is also a work contract.

        A stable key makes the send idempotent: the same key returns the original message.
        """
        if intent not in INTENTS:
            raise ValueError('intent must be handoff or notification')
        if not isinstance(body, str) or not body.strip() or len(body) > 200000:
            raise ValueError('Message body: 1-200000 characters')
        if not sender.startswith('service:') and not self.agent(sender):
            raise ValueError('Unknown sender ' + sender + '; register it first')
        if not self.agent(recipient):
            raise ValueError('Unknown recipient ' + recipient + '; register it first')
        if not self.allowed(sender, recipient):
            raise ValueError('The connection ' + sender + ' → ' + recipient + ' is not allowed')
        if due_seconds is not None and (not isinstance(due_seconds, int) or not 60 <= due_seconds <= 30 * 86400):
            raise ValueError('due_seconds must be an integer between 60 and 2592000')
        at = self.clock()
        mid = str(uuid.uuid4())
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if key is not None:
                old = db.execute('SELECT * FROM messages WHERE key=?', (key,)).fetchone()
                if old:
                    if (old['sender'], old['recipient'], old['body']) != (sender, recipient, body):
                        raise ValueError('That key was already used for a different message')
                    return old['id']
            db.execute('INSERT INTO messages(id,sender,recipient,body,intent,key,created) VALUES(?,?,?,?,?,?,?)',
                       (mid, sender, recipient, body, intent, key, at))
            if title is not None:
                title = ' '.join(str(title).split())[:120] or 'Work'
                db.execute('INSERT INTO work(message_id,title,due_seconds,created) VALUES(?,?,?,?)', (mid, title, due_seconds, at))
        return mid

    def message(self, mid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM messages WHERE id=?', (mid,)).fetchone()
        if not row:
            raise ValueError('Unknown message ' + str(mid))
        return dict(row)

    def inbox(self, agent_id, unread_only=True, limit=50):
        q = 'SELECT * FROM messages WHERE recipient=?' + (' AND read_at IS NULL' if unread_only else '') + ' ORDER BY created LIMIT ?'
        with self.connect() as db:
            return [dict(r) for r in db.execute(q, (agent_id, limit))]

    def mark_read(self, mid, agent_id):
        m = self.message(mid)
        if m['recipient'] != agent_id:
            raise ValueError('Only the recipient can read-acknowledge a message')
        with self.connect() as db:
            db.execute('UPDATE messages SET read_at=COALESCE(read_at,?) WHERE id=?', (self.clock(), mid))

    # ---- work contracts -----------------------------------------------------------
    def work(self, mid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM work WHERE message_id=?', (mid,)).fetchone()
        return self._work(row) if row else None

    @staticmethod
    def _work(row):
        w = dict(row)
        for k in ('result', 'closure'):
            w[k] = json.loads(w[k]) if w[k] else None
        return w

    def accept(self, mid, agent_id):
        m, w = self.message(mid), self.work(mid)
        if not w or m['recipient'] != agent_id:
            raise ValueError('Only the assigned agent can accept this work')
        with self.connect() as db:
            db.execute('UPDATE work SET accepted=COALESCE(accepted,?) WHERE message_id=?', (self.clock(), mid))
            db.execute('UPDATE messages SET read_at=COALESCE(read_at,?) WHERE id=?', (self.clock(), mid))

    def return_work(self, mid, agent_id, summary, evidence=None, blocked=False, route=None) -> str:
        """The assigned agent returns a result. The sender gets a return handoff."""
        m, w = self.message(mid), self.work(mid)
        if not w or m['recipient'] != agent_id:
            raise ValueError('Only the assigned agent can return this work')
        if w['returned']:
            raise ValueError('This work was already returned')
        summary = ' '.join(str(summary or '').split())
        if not summary:
            raise ValueError('A return needs a one-line summary')
        result = {'summary': summary[:2000], 'disposition': 'BLOCKED' if blocked else 'DONE',
                  'evidence': evidence, 'route': route}
        body = ('Returned: ' + w['title'] + '\n\n' + ('Blocked: ' if blocked else 'Result: ') + summary[:2000] +
                ('\n\nEvidence: ' + str(evidence)[:4000] if evidence else ''))
        at = self.clock()
        rid = str(uuid.uuid4())
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT returned FROM work WHERE message_id=?', (mid,)).fetchone()['returned']:
                raise ValueError('This work was already returned')
            db.execute('INSERT INTO messages(id,sender,recipient,body,intent,created) VALUES(?,?,?,?,?,?)',
                       (rid, agent_id, m['sender'], body, 'handoff' if not m['sender'].startswith('service:') else 'notification', at))
            db.execute('UPDATE work SET returned=?, result=?, return_message_id=?, accepted=COALESCE(accepted,?) WHERE message_id=?',
                       (at, json.dumps(result), rid, at, mid))
            db.execute('UPDATE messages SET read_at=COALESCE(read_at,?) WHERE id=?', (at, mid))
        return rid

    def close_work(self, mid, agent_id, outcome, note=''):
        """The original sender records a decision on the returned work."""
        m, w = self.message(mid), self.work(mid)
        if not w or m['sender'] != agent_id:
            raise ValueError('Only the original sender can close this work')
        if outcome not in CLOSE_OUTCOMES:
            raise ValueError('outcome must be accepted, revision or blocked')
        closure = {'outcome': outcome, 'note': ' '.join(str(note or '').split())[:1000], 'at': self.clock()}
        with self.connect() as db:
            db.execute('UPDATE work SET closed=?, closure=? WHERE message_id=?', (self.clock(), json.dumps(closure), mid))
            if w['return_message_id']:
                db.execute('UPDATE messages SET read_at=COALESCE(read_at,?) WHERE id=?', (self.clock(), w['return_message_id']))

    def work_for_return(self, return_mid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM work WHERE return_message_id=?', (return_mid,)).fetchone()
        return self._work(row) if row else None

    def work_open(self, mid) -> bool:
        """A work contract stays open until returned or closed; a plain message until read."""
        w = self.work(mid)
        if w:
            return not w['returned'] and not w['closed']
        return not self.message(mid)['read_at']

    def overdue(self):
        at = self.clock()
        with self.connect() as db:
            rows = db.execute('SELECT * FROM work WHERE due_seconds IS NOT NULL AND returned IS NULL AND closed IS NULL '
                              'AND overdue_alerted IS NULL AND created+due_seconds<=?', (at,)).fetchall()
        return [self._work(r) for r in rows]

    def mark_overdue_alerted(self, mid):
        with self.connect() as db:
            db.execute('UPDATE work SET overdue_alerted=? WHERE message_id=?', (self.clock(), mid))

    def work_list(self, limit=200):
        with self.connect() as db:
            rows = db.execute('SELECT w.*, m.sender, m.recipient FROM work w JOIN messages m ON m.id=w.message_id '
                              'ORDER BY w.created DESC LIMIT ?', (limit,)).fetchall()
        return [self._work(r) for r in rows]

    # ---- handoff rows -------------------------------------------------------------
    def handoff(self, hid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM handoffs WHERE id=?', (hid,)).fetchone()
        return self._handoff(row) if row else None

    @staticmethod
    def _handoff(row):
        h = dict(row)
        h['target'] = json.loads(h['target'])
        h['receipt'] = json.loads(h['receipt'])
        return h

    def handoffs(self, limit=200, statuses=None):
        q = 'SELECT * FROM handoffs'
        args = []
        if statuses:
            q += ' WHERE status IN (' + ','.join('?' * len(statuses)) + ')'
            args += list(statuses)
        q += ' ORDER BY created DESC LIMIT ?'
        with self.connect() as db:
            return [self._handoff(r) for r in db.execute(q, args + [limit])]

    def events(self, hid=None, after=0, limit=500):
        q = 'SELECT * FROM events WHERE sequence>?' + (' AND handoff_id=?' if hid else '') + ' ORDER BY sequence LIMIT ?'
        args = (after, hid, limit) if hid else (after, limit)
        with self.connect() as db:
            return [dict(r) for r in db.execute(q, args)]

    def release(self, hid, note='') -> dict:
        """An operator stops tracking one unfinished delivery. It becomes CANCELLED.

        Nothing is sent, resent or interrupted: a turn that is really still running keeps
        running, and the recipient's own activity still decides when the next handoff goes.
        Use it when a person knows how a stuck UNCERTAIN, ACCEPTED or RUNNING row ended.
        """
        at = self.clock()
        note = ' '.join(str(note or '').split())[:300]
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT status, receipt FROM handoffs WHERE id=?', (hid,)).fetchone()
            if not row:
                raise ValueError('Unknown handoff ' + str(hid))
            if row['status'] in TERMINAL:
                raise ValueError('That delivery already ended (' + row['status'] + '); there is nothing to release.')
            detail = ('Released by an operator while ' + row['status'] + '; nothing was resent.' +
                      (' Note: ' + note if note else ''))
            receipt = {**json.loads(row['receipt'] or '{}'), 'releasedFrom': row['status'], 'releasedAt': at,
                       'resendDecision': 'final'}
            db.execute('UPDATE handoffs SET status=?,detail=?,updated=?,next_check=?,receipt=? WHERE id=?',
                       ('CANCELLED', detail, at, at, json.dumps(receipt), hid))
            db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)', (hid, at, 'CANCELLED', detail))
            self._wake_queue(db, hid, at)
        return self.handoff(hid)

    @staticmethod
    def _wake_queue(db, hid, at):
        """The recipient's slot is free: check the handoffs queued behind it on the next pass."""
        db.execute("UPDATE handoffs SET next_check=? WHERE status='BUSY' AND next_check>? AND "
                   "recipient=(SELECT recipient FROM handoffs WHERE id=?)", (at, at, hid))

    def update(self, hid, status, detail, receipt=None, delay=10, keep=()):
        """Change a row's state and log the event. Keys named in keep survive a receipt replace."""
        if status not in STATES:
            raise ValueError('Unknown state ' + status)
        at = self.clock()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prev = db.execute('SELECT status,detail,receipt FROM handoffs WHERE id=?', (hid,)).fetchone()
            if not prev:
                raise ValueError('Unknown handoff ' + hid)
            db.execute('UPDATE handoffs SET status=?,detail=?,updated=?,next_check=? WHERE id=?', (status, detail, at, at + delay, hid))
            if receipt is not None:
                kept = json.loads(prev['receipt'] or '{}')
                receipt = {**{k: kept[k] for k in keep if k in kept}, **receipt}
                db.execute('UPDATE handoffs SET receipt=? WHERE id=?', (json.dumps(receipt), hid))
            if (prev['status'], prev['detail']) != (status, detail):
                db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)', (hid, at, status, detail))
            if status in TERMINAL and prev['status'] not in TERMINAL:
                self._wake_queue(db, hid, at)
