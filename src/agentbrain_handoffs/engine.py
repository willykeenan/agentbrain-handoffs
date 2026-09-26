"""The delivery engine: turns inbox handoffs into exactly one turn for the exact recipient.

Guarantees (each has a test):
1. Exact recipient. A handoff is delivered only to the agent it was addressed to, at the
   endpoint it had when enrolled. No substitute is ever chosen.
2. One provider write per attempt. The attempt is reserved atomically before sending;
   a lost reply makes the row UNCERTAIN, which is reconciled by observation, never resent.
   An explicit refusal is retried once; a second refusal proves the turn was never
   delivered, so the row ends FAILED (final) instead of holding the recipient.
3. Never interrupt. A busy recipient keeps its turn; handoffs to it queue as BUSY, and
   only one delivery per recipient is in flight at a time.
4. Automatic, bounded resends. A delivered turn that errored is resent up to 2 times; one
   that was interrupted once, after 10 minutes, and only while the work is still open.
   A resend is a new delivery with a new request id; the earlier turn is never replayed.
5. Honest release. An UNCERTAIN delivery that 3 complete history scans over an hour prove
   was never received is released (CANCELLED), not resent. An accepted turn that cannot be
   observed for 10 minutes becomes UNCERTAIN, so the same rule applies to it. An operator
   can release any unfinished row (`handoffs release`); nothing is resent either way.
6. Provider outages hold delivery without spending attempts (optional status-page guard).
7. Deadlines. Work past its due time alerts the sender once with a non-waking notification.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import time
import uuid
from pathlib import Path

from . import cards
from .store import ACTIVE, PENDING, TERMINAL, Store, sha256
from .transport import NotAccepted, Transport

SCAN_ROWS = 16
SCAN_SECONDS = 10
ABSENT_SCANS = 3
ABSENT_SECONDS = 3600
SENDING_TIMEOUT = 60
RESEND_MAX_FAILED = 2
RESEND_MAX_STOPPED = 1
RESEND_STOPPED_WAIT = 600
RESEND_WINDOW = 86400
RESEND_KEYS = ('resends', 'resentAfter', 'previousRequests')
UNOBSERVED_LIMIT = 600     # an ACCEPTED/RUNNING turn nobody can observe this long becomes UNCERTAIN
POLL_FAST = 2              # seconds between checks of a turn accepted in the last POLL_FAST_WINDOW
POLL_SLOW = 10
POLL_FAST_WINDOW = 120
HOST_WAIT = ('Waiting for a long-running engine ("handoffs run" or "handoffs serve"). A one-shot tick does not '
             'start {provider} turns: the turn runs inside the process that starts it and would stop when tick exits.')

INSTRUCTIONS = ('Take the next step for this assignment, then return evidence or the exact blocker '
                '(`handoffs return {mid} --as {agent} --summary "..."`, or the return_work MCP tool). '
                'Reading or ending your turn does not close work.')


class Engine:
    def __init__(self, store: Store, transport: Transport, clock=time.time, monotonic=time.monotonic,
                 outage=None, health_path=None, one_shot=False):
        """one_shot: this engine runs a single pass and exits (``handoffs tick``). It then never
        starts a turn for an adapter whose turns live inside the starting process (``needs_host``)."""
        self.store = store
        self.one_shot = one_shot
        self.transport = transport
        self.clock = clock
        self.monotonic = monotonic
        self.outage = outage  # callable -> str|None, see outage.py
        self.health_path = Path(health_path) if health_path else store.path.with_name(store.path.name + '.health.json')
        self.lock_path = store.path.with_name(store.path.name + '.lock')

    # ---- helpers ------------------------------------------------------------------
    def update(self, hid, status, detail, receipt=None, delay=10):
        self.store.update(hid, status, detail, receipt, delay, keep=RESEND_KEYS)

    def activity(self, agent):
        try:
            a = self.transport.activity(agent) or {}
        except Exception:
            a = {}
        return {'turnStatus': a.get('turnStatus', 'unknown'), 'turnId': a.get('turnId'), **a}

    def turn_still_open(self, h, obs):
        agent = self.store.agent(h['recipient'])
        if not agent:
            return False
        a = self.activity(agent)
        turn = (obs.get('receipt') or {}).get('turnId') or h['receipt'].get('turnId')
        return a['turnStatus'] == 'open' and (not a.get('turnId') or not turn or a['turnId'] == turn)

    def needs_host(self, agent) -> bool:
        """True when this agent's adapter runs turns inside the process that started them."""
        transport = self.transport
        with contextlib.suppress(Exception):
            if callable(getattr(transport, 'adapter', None)):
                transport = transport.adapter(agent)
        return bool(getattr(transport, 'needs_host', False))

    def poll_delay(self, receipt) -> float:
        """Check a freshly accepted turn often (short turns show as Working), older ones less."""
        since = receipt.get('acceptedAt')
        fresh = isinstance(since, (int, float)) and self.clock() - since < POLL_FAST_WINDOW
        return POLL_FAST if fresh else POLL_SLOW

    def close(self):
        """Close adapters that hold processes (for a one-shot engine that is about to exit)."""
        adapters = getattr(self.transport, 'adapters', None)
        for adapter in (adapters.values() if isinstance(adapters, dict) else [self.transport]):
            close = getattr(adapter, 'close', None)
            if callable(close):
                with contextlib.suppress(Exception):
                    close()

    @staticmethod
    def proven_absent(obs):
        search = (obs.get('receipt') or {}).get('historySearch') or {}
        return (not obs.get('status') and search.get('exhausted') is True and search.get('candidate') is None
                and not search.get('turnId'))

    # ---- enrollment ---------------------------------------------------------------
    def enroll_new(self, limit=64):
        """Every unread handoff message sent after delivery was enabled gets one row."""
        s = self.store.settings()
        after = float(s.get('enabledAfter') or 0)
        with self.store.connect() as db:
            # Skip mail for agents that no longer exist so a dead inbox cannot
            # consume the enrollment limit and starve live recipients.
            rows = db.execute("SELECT m.* FROM messages m LEFT JOIN handoffs h ON h.id=m.id "
                              "JOIN agents a ON a.id=m.recipient WHERE h.id IS NULL "
                              "AND m.intent='handoff' AND m.read_at IS NULL AND m.created>=? "
                              "ORDER BY m.created LIMIT ?",
                              (after, limit)).fetchall()
        enrolled = []
        for m in rows:
            try:
                enrolled.append(self.enroll(dict(m)))
            except ValueError:
                continue
        return enrolled

    def enroll(self, m):
        agent = self.store.agent(m['recipient'])
        if not agent:
            raise ValueError('Recipient is not registered')
        target = {k: agent.get(k) for k in ('id', 'provider', 'endpoint', 'cwd')}
        digest = sha256(m['body'])
        at = self.clock()
        live = PENDING + ACTIVE
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM handoffs WHERE id=?', (m['id'],)).fetchone():
                return m['id']
            # Only plain messages are deduplicated. Every work assignment and every return
            # belongs to its own work contract, so two with the same text are still two.
            linked = db.execute('SELECT 1 FROM work WHERE message_id=? OR return_message_id=?',
                                (m['id'], m['id'])).fetchone()
            dup = None if linked else db.execute(
                'SELECT h.id FROM handoffs h WHERE h.sender=? AND h.recipient=? AND h.body_sha=? AND h.status IN ('
                + ','.join('?' * len(live)) + ') AND NOT EXISTS (SELECT 1 FROM work w WHERE w.message_id=h.id '
                'OR w.return_message_id=h.id) LIMIT 1',
                (m['sender'], m['recipient'], digest, *live)).fetchone()
            status = 'DUPLICATE' if dup else 'WAITING'
            detail = ('Identical to handoff ' + dup['id'] + ', which is still in progress; not sent twice.'
                      if dup else 'Queued for delivery.')
            db.execute('INSERT INTO handoffs(id,sender,recipient,request_id,body_sha,target,status,detail,created,updated) '
                       'VALUES(?,?,?,?,?,?,?,?,?,?)', (m['id'], m['sender'], m['recipient'], str(uuid.uuid4()), digest,
                                                       json.dumps(target), status, detail, at, at))
            db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)', (m['id'], at, status, detail))
        return m['id']

    # ---- the message the recipient sees -------------------------------------------
    def envelope(self, h, m):
        store = self.store
        work = store.work(h['id'])
        returned = store.work_for_return(h['id'])
        if returned:
            card = cards.card(store.name(h['sender']), store.name(h['recipient']), kind='return', work=returned)
            instructions = ('Review the returned result, then record a decision '
                            '(`handoffs close ' + returned['message_id'] + ' --as ' + h['recipient'] +
                            ' accepted|revision|blocked`, or the close_work MCP tool).')
        else:
            due = None
            if work and work.get('due_seconds'):
                due = time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(work['created'] + work['due_seconds']))
            card = cards.card(store.name(h['sender']), store.name(h['recipient']), body=m['body'], work=work, due_at=due)
            instructions = INSTRUCTIONS.format(mid=h['id'], agent=h['recipient']) if work else 'Reply or act as the message asks.'
        return cards.envelope(card, h['request_id'], h['sender'], h['recipient'], h['id'], instructions)

    # ---- one row ------------------------------------------------------------------
    def step(self, h):
        m = self.store.message(h['id'])
        target = h['target']
        receipt = h['receipt']
        if h['status'] in ACTIVE:
            # A body change after the provider write must not misreport the
            # turn that already went out; reconcile that attempt instead.
            self.reconcile(h, target, receipt)
            return
        if sha256(m['body']) != h['body_sha']:
            self.update(h['id'], 'FAILED', 'The original message changed after it was queued; not delivered.')
            return
        if m['read_at'] and not (receipt.get('resends') and self.store.work_open(h['id'])):
            self.update(h['id'], 'ACKNOWLEDGED', 'The recipient already read the message; no extra turn needed.')
            return
        s = self.store.settings()
        if not s['enabled']:
            self.update(h['id'], 'HELD', 'Delivery is paused.', delay=30)
            return
        if not self.store.allowed(h['sender'], h['recipient']):
            self.update(h['id'], 'HELD', 'The connection from sender to recipient is not allowed.', delay=30)
            return
        agent = self.store.agent(h['recipient'])
        if not agent or agent.get('endpoint') != target.get('endpoint') or agent.get('provider') != target.get('provider'):
            self.update(h['id'], 'UNAVAILABLE', 'The exact recipient no longer resolves; no substitute agent is chosen.', delay=60)
            return
        if self.one_shot and self.needs_host(agent):
            self.update(h['id'], 'WAITING', HOST_WAIT.format(provider=agent.get('provider')), delay=30)
            return
        outage = self.outage() if self.outage else None
        if outage:
            self.update(h['id'], h['status'], outage, delay=60)  # no attempt spent
            return
        a = self.activity(agent)
        if a['turnStatus'] == 'open':
            self.update(h['id'], 'BUSY', 'The recipient is working; this waits for its current turn to finish.')
            return
        if a['turnStatus'] not in ('finished', 'stopped', 'failed'):
            self.update(h['id'], 'UNAVAILABLE', 'The recipient\'s activity cannot be verified; nothing is sent blind.', delay=30)
            return
        retry = receipt.get('rejectedBeforeAcceptance') is True and h['attempts'] == 1
        if not self.reserve(h, retry, dry=True):
            return
        with self.transport.prepare(agent) as prepared:
            # Preparing can take time: re-check permission and activity at the send boundary.
            if not self.store.settings()['enabled'] or not self.store.allowed(h['sender'], h['recipient']):
                self.update(h['id'], 'HELD', 'Delivery was paused or the connection was revoked during preparation.')
                return
            if self.activity(agent)['turnStatus'] not in ('finished', 'stopped', 'failed'):
                self.update(h['id'], 'BUSY', 'The recipient became busy during preparation.')
                return
            if not self.reserve(h, retry):
                return
            try:
                result = self.transport.start(prepared, self.envelope(h, m), h['request_id']) or {}
            except NotAccepted as e:
                if retry:
                    # Two explicit refusals prove no turn exists, so nothing is in flight:
                    # the row ends here and the recipient is free for later handoffs.
                    self.update(h['id'], 'FAILED', 'The agent app refused the turn twice before accepting it, so it '
                                'was not delivered: ' + str(e)[:300] + '. Not retried further; send it again when the '
                                'recipient can take it.',
                                {**receipt, 'rejection': str(e)[:300], 'resendDecision': 'final', 'notDelivered': True})
                else:
                    self.update(h['id'], 'OWNER_REJECTED', 'The agent app confirmed it did not accept the turn; one retry is allowed.',
                                {**receipt, 'rejectedBeforeAcceptance': True, 'rejection': str(e)[:300]}, delay=15)
                return
            except Exception as e:
                self.update(h['id'], 'UNCERTAIN', 'Delivery result unknown (' + str(e)[:300] + '); reconciled by observation, never resent.', receipt)
                return
            if result.get('turnId') and result.get('clientUserMessageId') == h['request_id']:
                self.update(h['id'], 'ACCEPTED', 'The agent app accepted one turn for the exact recipient.',
                            {**receipt, **result, 'rejectedBeforeAcceptance': False, 'acceptedAt': self.clock()},
                            delay=POLL_FAST)
            else:
                self.update(h['id'], 'UNCERTAIN', 'The agent app returned no exact turn receipt; reconciled by observation, never resent.',
                            {**receipt, **result})

    def reserve(self, h, retry, dry=False):
        """Atomically claim the single in-flight slot for this recipient (dry: check only)."""
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT status,attempts,detail FROM handoffs WHERE id=?', (h['id'],)).fetchone()
            if row['status'] not in PENDING or (row['attempts'] and not retry):
                return False
            blocker = db.execute('SELECT id,status FROM handoffs WHERE recipient=? AND id!=? AND status IN (?,?,?,?) '
                                 'ORDER BY created LIMIT 1', (h['recipient'], h['id'], *ACTIVE)).fetchone()
            at = self.clock()
            if blocker:
                detail = 'Waiting for the earlier delivery ' + blocker['id'] + ' (' + blocker['status'] + ') to this recipient.'
                if (row['status'], row['detail']) != ('BUSY', detail):
                    db.execute('UPDATE handoffs SET status=?,detail=?,updated=?,next_check=? WHERE id=?', ('BUSY', detail, at, at + 10, h['id']))
                    db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)', (h['id'], at, 'BUSY', detail))
                else:
                    # Unchanged, but still re-check it later: a waiting row that stayed due every
                    # pass would fill the scan and starve every other recipient.
                    db.execute('UPDATE handoffs SET next_check=? WHERE id=?', (at + 10, h['id']))
                return False
            if dry:
                return True
            db.execute("UPDATE handoffs SET status='SENDING',attempts=attempts+1,updated=?,next_check=? WHERE id=?", (at, at + 5, h['id']))
            db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)',
                       (h['id'], at, 'SENDING', 'One delivery attempt reserved.'))
        return True

    def reconcile(self, h, target, receipt):
        agent = self.store.agent(h['recipient']) or {'id': h['recipient'], **target}
        obs = self.transport.observe({**agent, **{k: target.get(k) for k in ('provider', 'endpoint', 'cwd')}}, h['request_id'], receipt) or {}
        merged = {**receipt, **(obs.get('receipt') or {})}
        status = obs.get('status')
        now = self.clock()
        if status in ('ACCEPTED', 'RUNNING', 'RETURNED', 'FAILED'):
            merged.pop('unobservedSince', None)
        if status == 'FAILED' and self.turn_still_open(h, obs):
            self.update(h['id'], 'RUNNING', 'Still running after a recoverable error; not treated as failed.', merged,
                        delay=self.poll_delay(merged))
        elif status in ('ACCEPTED', 'RUNNING', 'RETURNED', 'FAILED'):
            self.update(h['id'], status, obs.get('detail') or 'Observed in the agent app.', merged,
                        delay=self.poll_delay(merged) if status in ('ACCEPTED', 'RUNNING') else 30)
        elif h['status'] == 'SENDING' and now - h['updated'] > SENDING_TIMEOUT:
            self.update(h['id'], 'UNCERTAIN', 'No delivery receipt after sending; reconciled by observation, never resent.', merged)
        elif h['status'] == 'SENDING':
            # Leave `updated` at the reservation time so the send timeout can fire.
            return
        elif h['status'] == 'UNCERTAIN' and self.proven_absent(obs):
            first = receipt.get('firstAbsentAt') or self.clock()
            scans = int(receipt.get('absentScans') or 0) + 1
            merged.update(absentScans=scans, firstAbsentAt=first)
            if scans >= ABSENT_SCANS and self.clock() - first >= ABSENT_SECONDS:
                self.update(h['id'], 'CANCELLED', 'Released: ' + str(scans) + ' complete history scans over ' +
                            str(int((self.clock() - first) // 60)) + ' min found no turn, so it was never delivered. '
                            'Nothing was resent; the inbox message is unchanged.', merged)
            else:
                self.update(h['id'], 'UNCERTAIN', 'Not found in the recipient history yet (scan ' + str(scans) + '/' +
                            str(ABSENT_SCANS) + ').', merged, delay=300)
        elif h['status'] in ('ACCEPTED', 'RUNNING'):
            # The app took the turn but cannot say what became of it (for example the process
            # that watched it restarted). Past a bound this is UNCERTAIN: flagged for a person,
            # and releasable by complete history scans, still never resent.
            since = receipt.get('unobservedSince')
            since = since if isinstance(since, (int, float)) else now
            if now - since >= UNOBSERVED_LIMIT:
                merged.pop('unobservedSince', None)
                why = ' '.join(str(obs.get('detail') or 'the agent app gave no answer').split())[:200]
                self.update(h['id'], 'UNCERTAIN', 'The ' + h['status'].lower() + ' turn could not be observed for ' +
                            str(int((now - since) // 60)) + ' min (' + why + '); reconciled by observation, never '
                            'resent. If you know how it ended, release it with "handoffs release ' + h['id'] + '".',
                            merged)
            else:
                self.update(h['id'], h['status'], h['detail'], {**merged, 'unobservedSince': since}, delay=POLL_SLOW)
        elif obs.get('receipt'):
            self.update(h['id'], h['status'], obs.get('detail') or h['detail'], merged)
        else:
            self.update(h['id'], h['status'], h['detail'], delay=10)

    # ---- automatic resend ---------------------------------------------------------
    def review_failures(self):
        at = self.clock()
        with self.store.connect() as db:
            rows = db.execute("SELECT id FROM handoffs WHERE status='FAILED' AND updated>=? AND receipt NOT LIKE ? "
                              "ORDER BY updated LIMIT 16", (at - RESEND_WINDOW, '%"resendDecision"%')).fetchall()
        reviewed = 0
        for row in rows:
            h = self.store.handoff(row['id'])
            receipt = h['receipt']
            if not receipt.get('turnId') and not receipt.get('observedTurnId'):
                self.update(h['id'], 'FAILED', h['detail'], {**receipt, 'resendDecision': 'final'})
                continue  # failed before any delivery (e.g. body changed): nothing to resend
            agent = self.store.agent(h['recipient'])
            try:
                obs = self.transport.observe({**(agent or {}), **h['target']}, h['request_id'], receipt) or {}
            except Exception:
                continue
            if obs.get('status') in ('RETURNED', 'RUNNING') or (obs.get('status') == 'FAILED' and self.turn_still_open(h, obs)):
                status = 'RUNNING' if obs.get('status') == 'FAILED' else obs['status']
                self.update(h['id'], status, 'Corrected: the delivered turn did not fail (' + status.lower() + ').',
                            {**receipt, **(obs.get('receipt') or {})})
                reviewed += 1
                continue
            a = self.activity(agent) if agent else {'turnStatus': 'unknown'}
            stopped = (a['turnStatus'] == 'stopped' and a.get('turnId') is not None
                       and a.get('turnId') in (receipt.get('turnId'), receipt.get('observedTurnId')))
            kind = 'interrupted' if stopped or receipt.get('turnOutcome') == 'interrupted' else 'failed'
            done = int(receipt.get('resends') or 0)
            limit = RESEND_MAX_STOPPED if kind == 'interrupted' else RESEND_MAX_FAILED
            if kind == 'interrupted' and at - h['updated'] < RESEND_STOPPED_WAIT:
                continue
            if done >= limit or not self.store.work_open(h['id']):
                why = 'resend limit reached' if done >= limit else 'the work is already returned, closed or read'
                self.update(h['id'], 'FAILED', 'The delivered turn ' + kind + '; not resent: ' + why + '.',
                            {**receipt, 'resendDecision': 'final'})
                reviewed += 1
                continue
            fresh = str(uuid.uuid4())
            history = (receipt.get('previousRequests') or []) + [{'requestId': h['request_id'], 'turnId': receipt.get('turnId')}]
            new_receipt = {'resends': done + 1, 'resentAfter': kind, 'previousRequests': history}
            detail = ('Resending automatically after the delivered turn ' + kind + ' (' + str(done + 1) + '/' + str(limit) +
                      '). The earlier turn is not replayed.')
            with self.store.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                cur = db.execute("UPDATE handoffs SET status='WAITING',attempts=0,request_id=?,receipt=?,detail=?,updated=?,"
                                 "next_check=? WHERE id=? AND status='FAILED'", (fresh, json.dumps(new_receipt), detail, at, at, h['id']))
                if cur.rowcount:
                    db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)', (h['id'], at, 'WAITING', detail))
            reviewed += 1
        return reviewed

    # ---- deadlines ----------------------------------------------------------------
    def alert_overdue(self):
        sent = 0
        for w in self.store.overdue():
            m = self.store.message(w['message_id'])
            if not self.store.agent(m['sender']):
                self.store.mark_overdue_alerted(w['message_id'])
                continue
            body = cards.card(self.store.name(m['recipient']), self.store.name(m['sender']), kind='overdue', work=w)
            try:
                self.store.send('service:deadline', m['sender'], body, intent='notification',
                                key='overdue:' + w['message_id'])
                sent += 1
            except Exception:
                pass  # a blocked or invalid alert must not stall the rest of the tick
            self.store.mark_overdue_alerted(w['message_id'])
        return sent

    # ---- the loop -----------------------------------------------------------------
    def tick(self):
        """One bounded pass. Safe to run from several processes: only one works at a time."""
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open('a') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {'skipped': 'another engine is running'}
            started = self.monotonic()
            report = {'enrolled': len(self.enroll_new()), 'overdueAlerts': self.alert_overdue(), 'resends': 0, 'checked': 0, 'errors': []}
            with contextlib.suppress(Exception):
                report['resends'] = self.review_failures()
            with self.store.connect() as db:
                # Longest-due first, so rows that keep waiting cannot crowd out the rest.
                rows = [r['id'] for r in db.execute('SELECT id FROM handoffs WHERE status NOT IN (?,?,?,?,?) AND next_check<=? '
                                                    'ORDER BY next_check, created LIMIT ?', (*TERMINAL, self.clock(), SCAN_ROWS))]
            for hid in rows:
                if report['checked'] and self.monotonic() - started >= SCAN_SECONDS:
                    break
                h = self.store.handoff(hid)
                try:
                    self.step(h)
                except Exception as e:  # never lose a row to an adapter bug
                    now = self.store.handoff(hid) or h
                    # After a reserved write the persisted row is ACTIVE; a
                    # crash before the write leaves PENDING with attempts=0.
                    status = 'UNCERTAIN' if now['attempts'] and now['status'] in ACTIVE else 'UNAVAILABLE'
                    self.update(hid, status, 'Delivery check failed: ' + str(e)[:400], delay=30)
                    report['errors'].append({'id': hid, 'error': str(e)[:200]})
                report['checked'] += 1
            report.update(at=self.clock(), ok=not report['errors'], pid=os.getpid(),
                          elapsedSeconds=round(self.monotonic() - started, 3))
            tmp = self.health_path.with_name(self.health_path.name + '.' + uuid.uuid4().hex + '.tmp')
            tmp.write_text(json.dumps(report, indent=2) + '\n')
            tmp.replace(self.health_path)
            return report

    def run_forever(self, interval=5.0, stop=None):
        while not (stop and stop()):
            try:
                self.tick()
            except Exception:
                pass
            time.sleep(interval)
