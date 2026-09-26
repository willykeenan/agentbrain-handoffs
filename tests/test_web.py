"""Tests for the live page and the JSON API behind it.

A temporary database and a loopback server. Nothing here talks to a real
agent, and the Host-header checks never need a non-loopback NIC.
"""
from __future__ import annotations

import contextlib
import http.client
import json
import os
import stat
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from agentbrain_handoffs.store import STATES, Store, sha256
from agentbrain_handoffs.web import (
    PUBLIC_DETAILS, Dashboard, PAGE_PATH, ensure_token, loopback_host, loopback_peer, make_server,
    percentile, serve, token_path,
)

START = 1_800_000_000.0
PRIVACY = ('cwd', 'endpoint', 'settings', 'receipt', 'command', 'thread-1')
# Generic markers of private data. Names that are private to one maintainer never ship in
# the package, not even spelled in pieces: CI (or a maintainer) lists them, one per line or
# comma-separated, in HANDOFFS_PRIVATE_PATTERNS, and they are checked the same way.
BASE_SECRET_PATTERNS = ('/' + 'Users/', '@' + 'gmail', 'sk' + '-', 'xai' + '-', 'ghp' + '_')


def secret_patterns(environ=None):
    raw = (os.environ if environ is None else environ).get('HANDOFFS_PRIVATE_PATTERNS', '')
    extra = tuple(p.strip() for p in raw.replace(',', '\n').splitlines() if p.strip())
    return BASE_SECRET_PATTERNS + extra


SECRET_PATTERNS = secret_patterns()


class Clock:
    def __init__(self, now=START):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        return self.now


class CheapTransport:
    cheap_activity = True

    def adapter(self, agent):
        return self

    def activity(self, agent):
        return {'turnStatus': 'open', 'turnId': 'turn-1'}


class ExpensiveTransport:
    cheap_activity = False

    def adapter(self, agent):
        return self


class FakeEngine:
    def __init__(self, health_path=None, transport=None):
        self.health_path = health_path
        self.transport = transport or CheapTransport()
        self.ran = threading.Event()
        self.stopped = threading.Event()

    def activity(self, agent):
        return {'turnStatus': 'open', 'turnId': 'turn-1'}

    def run_forever(self, interval=2.0, stop=None):
        self.ran.set()
        while not (stop and stop()):
            time.sleep(0.01)
        self.stopped.set()


def request(server, method, path, headers=None, body=None):
    """One HTTP round-trip to a server bound on 127.0.0.1."""
    port = server.server_address[1]
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, {k.lower(): v for k, v in resp.getheaders()}, data
    finally:
        conn.close()


def json_body(data):
    return json.loads(data.decode('utf-8'))


def seed_handoff(store, mid, status='WAITING', detail='Queued for delivery.', attempts=0,
                 receipt=None, at=None, event_at=None):
    m = store.message(mid)
    when = store.clock() if at is None else at
    target = json.dumps({'id': m['recipient'], 'provider': 'demo', 'endpoint': '', 'cwd': ''})
    with store.connect() as db:
        db.execute(
            'INSERT INTO handoffs(id,sender,recipient,request_id,body_sha,target,status,attempts,'
            'created,updated,detail,receipt) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
            (mid, m['sender'], m['recipient'], str(uuid.uuid4()), sha256(m['body']), target,
             status, attempts, when, when, detail, json.dumps(receipt or {})),
        )
        db.execute('INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)',
                   (mid, when if event_at is None else event_at, status, detail))


class Helpers(unittest.TestCase):
    def test_percentile_empty_is_none(self):
        self.assertIsNone(percentile([], 0.5))
        self.assertIsNone(percentile([None], 0.9))

    def test_percentile_interpolates(self):
        self.assertEqual(percentile([10], 0.5), 10)
        self.assertEqual(percentile([10], 0.9), 10)
        self.assertEqual(percentile([1, 2, 3, 4], 0.5), 2.5)
        self.assertEqual(percentile([1, 2, 3, 4], 0.9), 3.7)

    def test_loopback_host_names(self):
        for header in ('127.0.0.1', '127.0.0.1:8765', 'localhost', 'LOCALHOST:9',
                       '[::1]', '[::1]:8765', '127.0.0.1.'):
            self.assertTrue(loopback_host(header), header)
        for header in ('example.com', '8.8.8.8', '192.168.1.1', '', None, '::1',
                       'evil.example:8765', '127.0.0.1.attacker.test'):
            self.assertFalse(loopback_host(header), header)

    def test_loopback_peer_addresses(self):
        self.assertTrue(loopback_peer('127.0.0.1'))
        self.assertTrue(loopback_peer('::1'))
        self.assertTrue(loopback_peer('::ffff:127.0.0.1'))
        self.assertFalse(loopback_peer('8.8.8.8'))
        self.assertFalse(loopback_peer('not-an-ip'))

    def test_token_created_mode_0600_and_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / 'h.sqlite3')
            first = ensure_token(store)
            path = token_path(store)
            self.assertTrue(path.exists())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(ensure_token(store), first)
            self.assertGreaterEqual(len(first), 16)
            self.assertEqual(path.name, 'h.sqlite3.token')

    def test_token_rejects_symlink_and_short_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / 'h.sqlite3')
            path = token_path(store)
            path.symlink_to('elsewhere')
            with self.assertRaises(ValueError):
                ensure_token(store)
            path.unlink()
            path.write_text('short\n')
            with self.assertRaises(ValueError):
                ensure_token(store)


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = Clock()
        self.store = Store(Path(self.tmp.name) / 'h.sqlite3', clock=self.clock)
        self.store.register_agent('planner', name='Planner', provider='demo')
        self.store.register_agent(
            'engineer', name='Engineer', provider='demo', cwd='/var/project',
            endpoint='thread-1', settings={'url': 'http://127.0.0.1/hook', 'token': 'not-for-the-page'},
        )
        self.dash = Dashboard(self.store, public_demo=False, clock=self.clock, cache_seconds=0)

    def test_status_has_spec_fields(self):
        mid = self.store.send('planner', 'engineer', 'Please fix login.', title='Fix login', due_seconds=3600)
        seed_handoff(self.store, mid, status='RUNNING', detail='The turn is running.', attempts=1)
        payload = self.dash.status()
        for key in ('agents', 'handoffs', 'work', 'metrics', 'health', 'generatedAt'):
            self.assertIn(key, payload)
        self.assertEqual(payload['generatedAt'], START)
        agent = payload['agents'][0]
        self.assertEqual(set(agent) >= {'id', 'name', 'provider', 'activity', 'state', 'openLoad'}, True)
        card = payload['handoffs'][0]
        for key in ('senderName', 'recipientName', 'title', 'needsAttention'):
            self.assertIn(key, card)
        self.assertEqual(card['senderName'], 'Planner')
        self.assertEqual(card['recipientName'], 'Engineer')
        self.assertEqual(card['title'], 'Fix login')
        item = payload['work'][0]
        self.assertIn('dueAt', item)
        self.assertIn('overdue', item)
        metrics = payload['metrics']
        self.assertEqual(set(metrics['counts']), set(STATES))
        self.assertIn('medianSeconds', metrics['toAccepted'])
        self.assertIn('p90Seconds', metrics['toAccepted'])
        self.assertIn('medianSeconds', metrics['toReturned'])
        self.assertIn('p90Seconds', metrics['toReturned'])
        self.assertIn('resends24h', metrics)
        self.assertIn('engineer', metrics['openLoad'])
        self.assertGreaterEqual(metrics['openLoad']['engineer'], 1)

    def test_status_hides_private_agent_fields(self):
        blob = json.dumps(self.dash.status())
        for word in PRIVACY + ('not-for-the-page', '/var/project'):
            self.assertNotIn(word, blob)

    def test_needs_attention_for_held_and_overdue(self):
        held = self.store.send('planner', 'engineer', 'Look at this held item.')
        seed_handoff(self.store, held, status='HELD', detail='Delivery is paused.')
        overdue = self.store.send('planner', 'engineer', 'This is late.', title='Late review', due_seconds=60)
        seed_handoff(self.store, overdue, status='WAITING', detail='Queued for delivery.')
        self.clock.advance(120)
        payload = self.dash.status()
        by_id = {h['id']: h for h in payload['handoffs']}
        self.assertTrue(by_id[held]['needsAttention'])
        self.assertTrue(by_id[overdue]['needsAttention'])
        self.assertTrue(by_id[overdue]['overdue'])
        work = {w['id']: w for w in payload['work']}
        self.assertTrue(work[overdue]['overdue'])
        self.assertEqual(work[overdue]['dueAt'], START + 60)
        self.assertGreaterEqual(payload['metrics']['needsAttention'], 2)

    def test_failed_waits_grace_unless_final(self):
        early = self.store.send('planner', 'engineer', 'Fresh failure.')
        seed_handoff(self.store, early, status='FAILED', detail='The turn failed.', receipt={})
        final = self.store.send('planner', 'engineer', 'Final failure.')
        seed_handoff(self.store, final, status='FAILED', detail='Not resent.',
                     receipt={'resendDecision': 'final'})
        payload = self.dash.status()
        by_id = {h['id']: h for h in payload['handoffs']}
        self.assertFalse(by_id[early]['needsAttention'])
        self.assertTrue(by_id[final]['needsAttention'])

    def test_latencies_and_resends_over_24h(self):
        mid = self.store.send('planner', 'engineer', 'Ship it.', title='Ship it')
        created = START
        seed_handoff(self.store, mid, status='WAITING', detail='Queued for delivery.', at=created)
        with self.store.connect() as db:
            db.execute("INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)",
                       (mid, created + 4, 'ACCEPTED', 'Accepted.'))
            db.execute("INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)",
                       (mid, created + 9, 'RETURNED', 'Done.'))
            db.execute("INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)",
                       (mid, created + 1, 'WAITING',
                        'Resending automatically after the delivered turn failed (1/2). The earlier turn is not replayed.'))
        payload = self.dash.status()
        self.assertEqual(payload['metrics']['toAccepted']['medianSeconds'], 4)
        self.assertEqual(payload['metrics']['toReturned']['medianSeconds'], 9)
        self.assertEqual(payload['metrics']['resends24h'], 1)

    def test_closed_work_is_included_with_open(self):
        open_id = self.store.send('planner', 'engineer', 'Still going.', title='Open task', due_seconds=600)
        closed_id = self.store.send('planner', 'engineer', 'Finished.', title='Closed task', due_seconds=600)
        self.store.accept(closed_id, 'engineer')
        self.store.return_work(closed_id, 'engineer', 'All good.')
        self.store.close_work(closed_id, 'planner', 'accepted', note='Looks right.')
        payload = self.dash.status()
        ids = {w['id'] for w in payload['work']}
        self.assertIn(open_id, ids)
        self.assertIn(closed_id, ids)
        closed = [w for w in payload['work'] if w['id'] == closed_id][0]
        self.assertEqual(closed['state'], 'closed')
        self.assertEqual(closed['outcome'], 'accepted')
        self.assertFalse(closed['overdue'])

    def test_health_is_trimmed(self):
        path = self.store.path.with_name(self.store.path.name + '.health.json')
        path.write_text(json.dumps({
            'at': START, 'ok': False, 'checked': 2, 'enrolled': 1, 'resends': 0,
            'overdueAlerts': 0, 'elapsedSeconds': 0.4,
            'errors': ['adapter exploded at /secret/local/path'],
        }))
        payload = self.dash.status()
        self.assertEqual(payload['health']['ok'], False)
        self.assertEqual(payload['health']['errors'], 1)
        self.assertEqual(payload['health']['checked'], 2)
        self.assertNotIn('/secret/local/path', json.dumps(payload))

    def test_cheap_activity_when_engine_present(self):
        engine = FakeEngine()
        dash = Dashboard(self.store, engine=engine, clock=self.clock, cache_seconds=0)
        payload = dash.status()
        engineer = [a for a in payload['agents'] if a['id'] == 'engineer'][0]
        self.assertEqual(engineer['activity'], 'open')
        self.assertEqual(engineer['state'], 'working')

    def test_expensive_adapter_skips_activity(self):
        engine = FakeEngine(transport=ExpensiveTransport())
        self.store.register_agent('coder', name='Coder', provider='codex')
        dash = Dashboard(self.store, engine=engine, clock=self.clock, cache_seconds=0)
        payload = dash.status()
        coder = [a for a in payload['agents'] if a['id'] == 'coder'][0]
        self.assertIsNone(coder['activity'])

    def test_command_http_activity_is_not_cheap(self):
        class Neutral:
            def adapter(self, agent):
                return self

        engine = FakeEngine(transport=Neutral())
        self.store.register_agent('hook', name='Hook', provider='command',
                                  settings={'url': 'http://127.0.0.1:9/hook'})
        self.store.register_agent('runner', name='Runner', provider='command',
                                  settings={'command': ['true']})
        dash = Dashboard(self.store, engine=engine, clock=self.clock, cache_seconds=0)
        self.assertFalse(dash._cheap(self.store.agent('hook')))
        self.assertTrue(dash._cheap(self.store.agent('runner')))
        self.assertTrue(dash._cheap(self.store.agent('engineer')))
        payload = dash.status()
        hook = [a for a in payload['agents'] if a['id'] == 'hook'][0]
        self.assertIsNone(hook['activity'])

    def test_events_tail_and_after(self):
        mid = self.store.send('planner', 'engineer', 'Hello there.')
        seed_handoff(self.store, mid, status='WAITING')
        with self.store.connect() as db:
            db.execute("INSERT INTO events(handoff_id,at,status,detail) VALUES(?,?,?,?)",
                       (mid, START + 1, 'BUSY', 'Recipient is busy.'))
        first = self.dash.events()
        self.assertGreaterEqual(len(first['events']), 1)
        self.assertIn('last', first)
        after = self.dash.events(after=0)
        self.assertTrue(any(e['status'] == 'BUSY' for e in after['events']))
        empty = self.dash.events(after=after['last'])
        self.assertEqual(empty['events'], [])
        self.assertEqual(empty['last'], after['last'])

    def test_send_creates_work_and_validates(self):
        result = self.dash.send({'from': 'planner', 'to': 'engineer', 'message': 'Do the thing.',
                                 'title': 'Do the thing', 'due_minutes': 5})
        self.assertIn('id', result)
        self.assertTrue(result['work'])
        self.assertIsNotNone(self.store.work(result['id']))
        with self.assertRaises(ValueError):
            self.dash.send({'from': 'planner', 'to': 'engineer'})
        with self.assertRaises(ValueError):
            self.dash.send({'from': 'planner', 'to': 'engineer', 'message': 'x', 'due_minutes': 'soon'})


class LiveServer(unittest.TestCase):
    public_demo = False

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = Clock()
        self.store = Store(Path(self.tmp.name) / 'h.sqlite3', clock=self.clock)
        self.store.register_agent('planner', name='Planner', provider='demo')
        self.store.register_agent('engineer', name='Engineer', provider='demo',
                                  cwd='/var/project', endpoint='thread-1')
        self.server = make_server(self.store, host='127.0.0.1', port=0,
                                  public_demo=self.public_demo, cache_seconds=0)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': 0.05}, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)
        self._wait()

    def _stop(self):
        with contextlib.suppress(Exception):
            self.server.shutdown()
        with contextlib.suppress(Exception):
            self.server.server_close()
        self.thread.join(timeout=2)

    def _wait(self):
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                status, _, _ = request(self.server, 'GET', '/healthz')
                if status == 200:
                    return
            except OSError:
                time.sleep(0.02)
        self.fail('server did not start')


class HttpTests(LiveServer):
    def test_get_page_and_index(self):
        for path in ('/', '/index.html'):
            status, headers, body = request(self.server, 'GET', path)
            self.assertEqual(status, 200)
            self.assertIn('text/html', headers['content-type'])
            self.assertIn(b'AgentBrain Handoffs', body)
            self.assertIn('content-security-policy', headers)
            self.assertEqual(headers.get('x-frame-options'), 'DENY')
            self.assertIn("frame-ancestors 'none'", headers['content-security-policy'])
            self.assertNotIn("'unsafe-inline'", headers['content-security-policy'])

    def test_page_bytes_match_file(self):
        status, _, body = request(self.server, 'GET', '/')
        self.assertEqual(status, 200)
        self.assertEqual(body, PAGE_PATH.read_bytes())

    def test_api_status_shape(self):
        mid = self.store.send('planner', 'engineer', 'A message with a title line.')
        seed_handoff(self.store, mid, status='ACCEPTED', detail='The provider accepted the turn.')
        self.server.dashboard.invalidate()
        status, headers, body = request(self.server, 'GET', '/api/status')
        self.assertEqual(status, 200)
        self.assertIn('application/json', headers['content-type'])
        payload = json_body(body)
        for key in ('agents', 'handoffs', 'work', 'metrics', 'health', 'generatedAt'):
            self.assertIn(key, payload)
        card = payload['handoffs'][0]
        self.assertEqual(card['senderName'], 'Planner')
        self.assertEqual(card['recipientName'], 'Engineer')
        self.assertIn('title', card)
        self.assertIn('needsAttention', card)
        blob = body.decode()
        for word in ('/var/project', 'thread-1', '/' + 'Users/'):
            self.assertNotIn(word, blob)

    def test_non_loopback_host_is_forbidden(self):
        status, headers, body = request(self.server, 'GET', '/api/status', headers={'Host': 'evil.example'})
        self.assertEqual(status, 403)
        self.assertIn(b'127.0.0.1', body)
        self.assertNotIn(('/' + 'Users/').encode(), body)
        text = body.decode()
        self.assertNotIn(str(self.store.path), text)

    def test_localhost_host_is_allowed(self):
        port = self.server.server_address[1]
        status, _, body = request(self.server, 'GET', '/healthz',
                                  headers={'Host': 'localhost:' + str(port)})
        self.assertEqual(status, 200)
        self.assertEqual(json_body(body)['ok'], True)

    def test_send_requires_token_then_writes(self):
        payload = json.dumps({'from': 'planner', 'to': 'engineer', 'message': 'Please review.'}).encode()
        status, _, body = request(self.server, 'POST', '/api/send',
                                  headers={'Content-Type': 'application/json'}, body=payload)
        self.assertEqual(status, 401, body)
        status, _, body = request(self.server, 'POST', '/api/send',
                                  headers={'Content-Type': 'application/json',
                                           'X-Handoffs-Token': 'nope'},
                                  body=payload)
        self.assertEqual(status, 403, body)
        token = token_path(self.store).read_text().strip()
        status, _, body = request(self.server, 'POST', '/api/send',
                                  headers={'Content-Type': 'application/json',
                                           'X-Handoffs-Token': token},
                                  body=payload)
        self.assertEqual(status, 201, body)
        created = json_body(body)
        self.assertTrue(created['id'])
        self.assertFalse(created['work'])
        self.assertEqual(self.store.message(created['id'])['body'], 'Please review.')

    def test_send_wrong_length_token_is_forbidden_not_500(self):
        payload = json.dumps({'from': 'planner', 'to': 'engineer', 'message': 'Hi'}).encode()
        status, _, _ = request(self.server, 'POST', '/api/send',
                               headers={'Content-Type': 'application/json',
                                        'X-Handoffs-Token': 'x'},
                               body=payload)
        self.assertEqual(status, 403)

    def test_send_accepts_token_with_surrounding_whitespace(self):
        payload = json.dumps({'from': 'planner', 'to': 'engineer', 'message': 'Padded token.'}).encode()
        token = token_path(self.store).read_text().strip()
        status, _, body = request(self.server, 'POST', '/api/send',
                                  headers={'Content-Type': 'application/json; charset=utf-8',
                                           'X-Handoffs-Token': '  ' + token + '  '},
                                  body=payload)
        self.assertEqual(status, 201, body)

    def test_send_rejects_non_json_content_type(self):
        token = token_path(self.store).read_text().strip()
        status, _, _ = request(self.server, 'POST', '/api/send',
                               headers={'Content-Type': 'text/plain',
                                        'X-Handoffs-Token': token},
                               body=b'{}')
        self.assertEqual(status, 415)

    def test_events_query(self):
        mid = self.store.send('planner', 'engineer', 'Evented.')
        seed_handoff(self.store, mid, status='WAITING')
        status, _, body = request(self.server, 'GET', '/api/events')
        self.assertEqual(status, 200)
        payload = json_body(body)
        self.assertIn('events', payload)
        self.assertTrue(payload['events'])
        last = payload['last']
        status, _, body = request(self.server, 'GET', '/api/events?after=' + str(last))
        self.assertEqual(json_body(body)['events'], [])
        status, _, body = request(self.server, 'GET', '/api/events?after=-1')
        self.assertEqual(status, 400)

    def test_not_found_and_method_and_head(self):
        status, _, _ = request(self.server, 'GET', '/nope')
        self.assertEqual(status, 404)
        status, _, _ = request(self.server, 'PUT', '/api/status')
        self.assertEqual(status, 405)
        status, headers, body = request(self.server, 'HEAD', '/')
        self.assertEqual(status, 200)
        self.assertEqual(body, b'')
        self.assertIn('text/html', headers['content-type'])

    def test_healthz(self):
        status, _, body = request(self.server, 'GET', '/healthz')
        self.assertEqual(status, 200)
        self.assertEqual(json_body(body), {'ok': True})


class PublicDemoTests(LiveServer):
    public_demo = True

    def test_any_host_is_served(self):
        status, headers, body = request(self.server, 'GET', '/', headers={'Host': 'space.example'})
        self.assertEqual(status, 200)
        self.assertIn(b'AgentBrain Handoffs', body)
        self.assertNotIn('x-frame-options', headers)
        self.assertNotIn("frame-ancestors 'none'", headers['content-security-policy'])

    def test_send_is_disabled(self):
        token = ensure_token(self.store)
        payload = json.dumps({'from': 'planner', 'to': 'engineer', 'message': 'Nope.'}).encode()
        status, _, body = request(self.server, 'POST', '/api/send',
                                  headers={'Content-Type': 'application/json',
                                           'X-Handoffs-Token': token},
                                  body=payload)
        self.assertEqual(status, 403)
        self.assertIn(b'public demo', body.lower())
        self.assertEqual(self.store.inbox('engineer'), [])

    def test_delivery_details_are_plain_state_sentences_not_adapter_errors(self):
        private = '[Errno 2] No such file or directory: ' + "'/srv/agents/private-agent.py'"
        mid = self.store.send('planner', 'engineer', 'Run the private agent.')
        seed_handoff(self.store, mid, status='FAILED', detail='The agent app refused the turn twice before '
                     'accepting it, so it was not delivered: ' + private + '. Not retried further.')
        self.server.dashboard.invalidate()
        for path in ('/api/status', '/api/events'):
            status, _, body = request(self.server, 'GET', path, headers={'Host': 'space.example'})
            self.assertEqual(status, 200)
            self.assertNotIn('private-agent.py', body.decode())
            self.assertNotIn('/srv/agents', body.decode())
        card = json_body(request(self.server, 'GET', '/api/status')[2])['handoffs'][0]
        self.assertEqual(card['detail'], PUBLIC_DETAILS['FAILED'])
        self.assertEqual(set(PUBLIC_DETAILS), set(STATES))  # every state has a public sentence

    def test_a_database_with_real_agents_is_never_served_publicly(self):
        self.store.register_agent('builder', name='Builder', provider='command',
                                  settings={'command': ['python3', 'agent.py']})
        with self.assertRaises(ValueError) as caught:
            make_server(self.store, host='127.0.0.1', port=0, public_demo=True)
        self.assertIn('builder', str(caught.exception))
        local = make_server(self.store, host='127.0.0.1', port=0)  # the owner's loopback page is fine
        local.server_close()


class PageTests(unittest.TestCase):
    def setUp(self):
        self.page = PAGE_PATH.read_text(encoding='utf-8')

    def test_self_contained_and_calm(self):
        page = self.page
        self.assertIn('prefers-color-scheme: dark', page)
        self.assertIn('max-width: 390px', page)
        self.assertIn('width=device-width', page)
        self.assertIn('In flight', page)
        self.assertIn('Needs attention', page)
        self.assertIn('Median time to accept', page)
        self.assertIn('Resends in 24 h', page)
        self.assertIn('/api/status', page)
        self.assertIn('3000', page)
        self.assertIn('needsAttention', page)
        self.assertIn('openLoad', page)
        self.assertIn('dueAt', page)
        self.assertIn('item.overdue', page)
        self.assertIn('Was due', page)
        self.assertNotRegex(page, r'<script\s+src=')
        self.assertNotRegex(page, r'<link\s')
        self.assertNotIn('googleapis', page)
        self.assertNotIn('cdn.', page)
        self.assertNotRegex(page, r'style=')
        for pat in SECRET_PATTERNS:
            self.assertNotIn(pat, page)

    def test_extra_private_patterns_come_from_the_environment(self):
        self.assertEqual(secret_patterns({}), BASE_SECRET_PATTERNS)
        extra = secret_patterns({'HANDOFFS_PRIVATE_PATTERNS': 'first name, internal-tool\nsecond'})
        self.assertEqual(extra[len(BASE_SECRET_PATTERNS):], ('first name', 'internal-tool', 'second'))

    def test_no_private_data_in_the_package(self):
        """Every shipped text file, against the generic patterns plus any CI-provided ones."""
        root = Path(__file__).resolve().parents[1]
        shipped = [root / name for name in ('README.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'SECURITY.md',
                                            'pyproject.toml', '.gitignore')]
        for folder in ('src', 'docs', 'tests', 'space', 'tools', '.github'):
            shipped += [f for f in (root / folder).rglob('*') if f.is_file() and '__pycache__' not in f.parts]
        # The API-key prefix marker also matches ordinary hyphenated words, so it is only
        # checked where the page and server code live (below); names are checked everywhere.
        names = [pat for pat in SECRET_PATTERNS if pat not in ('sk' + '-',)]
        for path in shipped:
            text = path.read_text(encoding='utf-8', errors='replace')
            for pat in names:
                self.assertNotIn(pat, text, str(path.relative_to(root)))

    def test_no_private_data_in_owned_sources(self):
        root = PAGE_PATH.parent
        for name in ('page.html', 'web.py'):
            text = (root / name).read_text(encoding='utf-8')
            for pat in SECRET_PATTERNS:
                self.assertNotIn(pat, text, name)
        tests = Path(__file__).read_text(encoding='utf-8')
        for pat in SECRET_PATTERNS:
            self.assertNotIn(pat, tests)

    def test_csp_hashes_cover_inline_script_and_style(self):
        from agentbrain_handoffs.web import _csp
        policy = _csp(self.page, False)
        self.assertIn('script-src', policy)
        self.assertIn('sha256-', policy)
        self.assertNotIn("'none'", policy.split('script-src')[1].split(';')[0])
        self.assertNotIn("'none'", policy.split('style-src')[1].split(';')[0])


class ServeTests(unittest.TestCase):
    def test_serve_ready_runs_engine_and_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(Path(tmp) / 'h.sqlite3')
            store.register_agent('planner', name='Planner')
            engine = FakeEngine()
            holder = {}

            def ready(server):
                holder['url'] = server.url

                def hit():
                    deadline = time.time() + 5
                    while time.time() < deadline:
                        try:
                            status, _, body = request(server, 'GET', '/')
                            if status == 200:
                                holder['body'] = body
                                break
                        except OSError:
                            time.sleep(0.03)
                    server.shutdown()

                threading.Thread(target=hit, daemon=True).start()

            serve(store, engine=engine, host='127.0.0.1', port=0, ready=ready, interval=0.05)
            self.assertTrue(engine.ran.wait(2))
            self.assertIn(b'AgentBrain Handoffs', holder.get('body', b''))
            self.assertTrue(holder['url'].startswith('http://127.0.0.1:'))
            engine.stopped.wait(2)


if __name__ == '__main__':
    unittest.main()
