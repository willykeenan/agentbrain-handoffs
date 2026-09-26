"""Tests for the provider status-page outage guard.

Everything stays offline: the guard is given a fake fetch, never a real URL.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from agentbrain_handoffs import outage
from agentbrain_handoffs.engine import Engine
from agentbrain_handoffs.store import Store
from agentbrain_handoffs.transport import Transport


class Clock:
    def __init__(self, at=1_800_000_000.0):
        self.at = at

    def __call__(self):
        return self.at


class FakeTransport(Transport):
    """Always idle, accepts every turn, and records what it sent."""
    name = 'fake'

    def __init__(self):
        self.sent = []

    def activity(self, agent):
        return {'turnStatus': 'finished', 'turnId': None}

    def start(self, prepared, text, request_id):
        self.sent.append((prepared['agent']['id'], text, request_id))
        return {'turnId': 'turn-' + request_id, 'clientUserMessageId': request_id}

    def observe(self, agent, request_id, receipt):
        return {'status': 'RUNNING', 'detail': 'Working.', 'receipt': {}}


class OutageGuardTests(unittest.TestCase):
    def summary(self, component_status, incidents=()):
        return {'components': [{'name': 'Codex API', 'status': component_status},
                               {'name': 'Sora', 'status': 'major_outage'}],
                'incidents': list(incidents)}

    def guard(self, document, **options):
        fetches = []

        def fetch(url):
            fetches.append(url)
            if isinstance(document, Exception):
                raise document
            return document
        guard = outage.StatusPageGuard('https://status.example.com/api/v2/summary.json', ['Codex API'],
                                       fetch=fetch, **options)
        return guard, fetches

    def test_holds_during_a_major_or_full_outage_of_a_watched_component(self):
        for status in ('major_outage', 'Major Outage', 'full_outage', 'full-outage'):
            reason = self.guard(self.summary(status))[0]()
            self.assertIn('Codex API', reason)
            self.assertIn('No attempt', reason)
            self.assertNotIn('\n', reason)

    def test_does_not_hold_for_lesser_problems_or_other_components(self):
        for status in ('operational', 'degraded_performance', 'partial_outage', 'under_maintenance', None):
            self.assertIsNone(self.guard(self.summary(status))[0]())

    def test_a_recovering_incident_releases_the_hold(self):
        by_keyword = {'name': 'Elevated errors in Codex', 'status': 'monitoring'}
        by_component = {'name': 'Elevated errors', 'status': 'resolved', 'components': [{'name': 'Codex API'}]}
        unrelated = {'name': 'Sora is slow', 'status': 'monitoring'}
        still_open = {'name': 'Codex is down', 'status': 'investigating'}
        self.assertIsNone(self.guard(self.summary('major_outage', [by_keyword]), incident_keyword='codex')[0]())
        self.assertIsNone(self.guard(self.summary('major_outage', [by_component]))[0]())
        self.assertIsNotNone(self.guard(self.summary('major_outage', [unrelated]), incident_keyword='codex')[0]())
        self.assertIsNotNone(self.guard(self.summary('major_outage', [still_open]), incident_keyword='codex')[0]())

    def test_fails_open(self):
        for broken in (OSError('network down'), ValueError('bad json'), {'components': 'nonsense'}, ['not', 'a', 'dict']):
            self.assertIsNone(self.guard(broken)[0]())

    def test_answers_are_cached_for_the_ttl(self):
        clock = Clock()
        guard, fetches = self.guard(self.summary('major_outage'), ttl=60, clock=clock)
        guard()
        clock.at += 59
        guard()
        self.assertEqual(len(fetches), 1)
        clock.at += 2
        guard()
        self.assertEqual(len(fetches), 2)

    def test_from_settings(self):
        self.assertIsNone(outage.from_settings(None))
        preset = outage.from_settings({'preset': 'openai-codex', 'ttl': 30})
        self.assertEqual(preset.url, outage.OPENAI_STATUS_URL)
        self.assertEqual(preset.incident_keyword, 'codex')
        self.assertEqual(preset.ttl, 30)
        custom = outage.from_settings({'url': 'https://status.example.com/api/v2/summary.json',
                                       'components': ['API', 'Workers'], 'incident_keyword': 'API'})
        self.assertEqual((custom.components, custom.incident_keyword), (('API', 'Workers'), 'api'))
        for bad in ({'preset': 'nope'}, {'preset': 'openai-codex', 'url': 'x'}, {'url': 'https://x'},
                    {'url': 'file:///etc/passwd', 'components': ['A']}, {'url': 'https://x', 'components': []},
                    {'url': 'https://x', 'components': ['A'], 'extra': 1}, 'openai-codex'):
            with self.assertRaises(ValueError, msg=bad):
                outage.from_settings(bad)

    def test_openai_codex_preset(self):
        guard = outage.openai_codex(ttl=15, fetch=lambda url: {'components': []})
        self.assertEqual(guard.url, outage.OPENAI_STATUS_URL)
        self.assertEqual(guard.components, outage.OPENAI_CODEX_COMPONENTS)
        self.assertEqual(guard.incident_keyword, 'codex')
        self.assertEqual(guard.ttl, 15)
        self.assertIsNone(guard())

    def test_rejects_non_http_urls(self):
        with self.assertRaises(ValueError):
            outage.StatusPageGuard('ftp://status.example.com/summary.json', ['API'])
        with self.assertRaises(ValueError):
            outage.StatusPageGuard('https://status.example.com/summary.json', [])
        with self.assertRaises(ValueError):
            outage.StatusPageGuard('https://status.example.com/summary.json', ['  '])

    def test_the_engine_holds_without_spending_an_attempt(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = Store(os.path.join(tmp.name, 'h.sqlite3'))
        store.register_agent('a', provider='fake')
        store.register_agent('b', provider='fake')
        mid = store.send('a', 'b', 'hello')
        reason = {'value': 'Held: the provider status page reports an outage (Codex API).'}
        transport = FakeTransport()
        engine = Engine(store, transport, outage=lambda: reason['value'])
        engine.tick()
        row = store.handoff(mid)
        self.assertEqual((row['status'], row['attempts'], transport.sent), ('WAITING', 0, []))
        self.assertIn('outage', row['detail'])
        reason['value'] = None
        with store.connect() as db:
            db.execute('UPDATE handoffs SET next_check=0')
        engine.tick()
        self.assertEqual((store.handoff(mid)['status'], len(transport.sent)), ('ACCEPTED', 1))

    def test_hold_reason_is_one_line_and_names_every_down_component(self):
        document = {'components': [{'name': 'Codex API', 'status': 'major_outage'},
                                   {'name': 'CLI', 'status': 'full_outage'}],
                    'incidents': []}
        fetches = []

        def fetch(url):
            fetches.append(url)
            return document

        guard = outage.StatusPageGuard('https://status.example.com/api/v2/summary.json',
                                       ['Codex API', 'CLI'], fetch=fetch)
        reason = guard()
        self.assertIn('Codex API', reason)
        self.assertIn('CLI', reason)
        self.assertEqual(reason.count('\n'), 0)


if __name__ == '__main__':
    unittest.main()
