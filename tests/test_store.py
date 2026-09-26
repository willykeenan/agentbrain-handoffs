"""Store validation: agents, connections, inbox, work permissions and handoff rows.

These tests exercise the public Store API only. They use a temporary SQLite file
and a settable clock; nothing here talks to an agent app.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from agentbrain_handoffs.store import (  # noqa: E402
    ACTIVE, AGENT_ID, CLOSE_OUTCOMES, DEFAULT_SETTINGS, PENDING, STATES, TERMINAL,
    Store, sha256,
)


START = 1_800_000_000.0


class Clock:
    def __init__(self, now=START):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        return self.now


class StoreTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.clock = Clock()
        self.store = Store(self.dir / 'handoffs.sqlite3', clock=self.clock)

    def team(self, *ids):
        ids = ids or ('planner', 'engineer', 'reviewer')
        for agent_id in ids:
            self.store.register_agent(agent_id, agent_id.title(), provider='demo', endpoint='thread-' + agent_id)
        return ids


class ConstructionTests(StoreTest):
    def test_database_file_is_created_private(self):
        path = self.store.path
        self.assertTrue(path.is_file())
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_symlink_database_is_rejected(self):
        target = self.dir / 'real.sqlite3'
        target.write_text('x')
        link = self.dir / 'link.sqlite3'
        os.symlink(target, link)
        with self.assertRaises(ValueError) as err:
            Store(link, clock=self.clock)
        self.assertIn('symlink', str(err.exception))

    def test_default_settings_are_filled_in(self):
        self.assertEqual(self.store.settings()['enabled'], True)
        self.assertEqual(self.store.settings()['enabledAfter'], 0.0)
        self.assertEqual(self.store.settings()['connections'], 'open')
        for key in DEFAULT_SETTINGS:
            self.assertIn(key, self.store.settings())


class AgentTests(StoreTest):
    def test_register_round_trip_and_upsert(self):
        row = self.store.register_agent('eng', 'Engineer', provider='fake', endpoint='thread-1', cwd='/tmp',
                                        settings={'timeout': 9})
        self.assertEqual(row['id'], 'eng')
        self.assertEqual(row['name'], 'Engineer')
        self.assertEqual(row['provider'], 'fake')
        self.assertEqual(row['endpoint'], 'thread-1')
        self.assertEqual(row['cwd'], '/tmp')
        self.assertEqual(row['settings'], {'timeout': 9})
        self.assertEqual(self.store.agent('eng')['name'], 'Engineer')
        self.store.register_agent('eng', 'Eng 2', provider='demo', endpoint='thread-2')
        self.assertEqual(self.store.agent('eng')['name'], 'Eng 2')
        self.assertEqual(self.store.agent('eng')['provider'], 'demo')
        self.assertEqual([a['id'] for a in self.store.agents()], ['eng'])

    def test_agent_id_must_match_the_published_pattern(self):
        self.assertTrue(AGENT_ID.fullmatch('a'))
        self.assertTrue(AGENT_ID.fullmatch('Planner_1:ops@org/team'))
        for bad in ('', '-leading', 'has space', 'bad!', 'x' * 201):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.store.register_agent(bad, 'X')
        self.assertIsNone(self.store.agent('missing'))
        self.store.register_agent('temp', 'Temp')
        self.store.remove_agent('temp')
        self.assertIsNone(self.store.agent('temp'))

    def test_service_names_are_stable(self):
        self.assertEqual(self.store.name('service:deadline'), 'Deadline service')
        self.assertEqual(self.store.name('service:router'), 'Flow router')
        self.assertEqual(self.store.name('service:other'), 'Service')
        self.assertEqual(self.store.name('ghost'), 'Unknown agent')
        self.store.register_agent('planner', 'Planner')
        self.assertEqual(self.store.name('planner'), 'Planner')


class SettingsTests(StoreTest):
    def test_configure_rejects_unknown_keys_and_bad_connection_mode(self):
        with self.assertRaises(ValueError):
            self.store.configure(nope=True)
        with self.assertRaises(ValueError):
            self.store.configure(connections='maybe')
        out = self.store.configure(enabled=False, enabledAfter=12.5, connections='explicit')
        self.assertEqual(out['enabled'], False)
        self.assertEqual(out['enabledAfter'], 12.5)
        self.assertEqual(out['connections'], 'explicit')
        self.assertEqual(self.store.settings()['connections'], 'explicit')


class ConnectionTests(StoreTest):
    def test_self_send_is_never_allowed(self):
        self.team('planner')
        self.assertFalse(self.store.allowed('planner', 'planner'))
        self.store.set_connection('planner', 'planner', allow=True)
        self.assertFalse(self.store.allowed('planner', 'planner'))

    def test_open_mode_allows_registered_pairs_until_blocked(self):
        self.team('planner', 'engineer')
        self.assertTrue(self.store.allowed('planner', 'engineer'))
        self.store.set_connection('planner', 'engineer', allow=False)
        self.assertFalse(self.store.allowed('planner', 'engineer'))
        self.store.set_connection('planner', 'engineer', allow=True)
        self.assertTrue(self.store.allowed('planner', 'engineer'))

    def test_explicit_mode_requires_an_allow_row(self):
        self.team('planner', 'engineer')
        self.store.configure(connections='explicit')
        self.assertFalse(self.store.allowed('planner', 'engineer'))
        self.store.set_connection('planner', 'engineer', allow=True)
        self.assertTrue(self.store.allowed('planner', 'engineer'))

    def test_service_senders_are_allowed_unless_explicitly_blocked(self):
        self.team('planner')
        self.store.configure(connections='explicit')
        self.assertTrue(self.store.allowed('service:deadline', 'planner'))
        self.store.set_connection('service:deadline', 'planner', allow=False)
        self.assertFalse(self.store.allowed('service:deadline', 'planner'))
        rows = self.store.connections()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['sender'], 'service:deadline')
        self.assertEqual(rows[0]['allow'], 0)


class MessageTests(StoreTest):
    def setUp(self):
        super().setUp()
        self.team()

    def test_send_validates_intent_body_agents_and_due(self):
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'hi', intent='whisper')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', '   ')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'x' * 200001)
        with self.assertRaises(ValueError):
            self.store.send('ghost', 'engineer', 'hi')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'ghost', 'hi')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'planner', 'hi')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'hi', due_seconds=59)
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'hi', due_seconds=30 * 86400 + 1)
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'hi', due_seconds=60.0)
        mid = self.store.send('planner', 'engineer', 'Build it.', title='Parser', due_seconds=60)
        row = self.store.message(mid)
        self.assertEqual((row['sender'], row['recipient'], row['intent']), ('planner', 'engineer', 'handoff'))
        self.assertEqual(row['body'], 'Build it.')
        self.assertIsNone(row['read_at'])
        work = self.store.work(mid)
        self.assertEqual(work['title'], 'Parser')
        self.assertEqual(work['due_seconds'], 60)

    def test_service_sender_need_not_be_registered(self):
        mid = self.store.send('service:deadline', 'planner', 'The parser is overdue.', intent='notification')
        row = self.store.message(mid)
        self.assertEqual(row['sender'], 'service:deadline')
        self.assertEqual(row['intent'], 'notification')

    def test_blocked_connection_cannot_send(self):
        self.store.set_connection('planner', 'engineer', allow=False)
        with self.assertRaises(ValueError) as err:
            self.store.send('planner', 'engineer', 'nope')
        self.assertIn('not allowed', str(err.exception))

    def test_idempotent_key_returns_the_original_and_rejects_a_mismatch(self):
        first = self.store.send('planner', 'engineer', 'Ship v1.', key='release-v1')
        again = self.store.send('planner', 'engineer', 'Ship v1.', key='release-v1')
        self.assertEqual(first, again)
        with self.assertRaises(ValueError):
            self.store.send('planner', 'engineer', 'Ship v2.', key='release-v1')
        with self.assertRaises(ValueError):
            self.store.send('planner', 'reviewer', 'Ship v1.', key='release-v1')
        other = self.store.send('planner', 'engineer', 'Ship v1.', key='release-v1-b')
        self.assertNotEqual(first, other)

    def test_inbox_unread_and_mark_read_is_recipient_only(self):
        mid = self.store.send('planner', 'engineer', 'Please review.')
        self.assertEqual([m['id'] for m in self.store.inbox('engineer')], [mid])
        self.assertEqual(self.store.inbox('planner'), [])
        with self.assertRaises(ValueError):
            self.store.mark_read(mid, 'planner')
        with self.assertRaises(ValueError):
            self.store.message('missing')
        self.store.mark_read(mid, 'engineer')
        self.assertEqual(self.store.inbox('engineer'), [])
        self.assertEqual(len(self.store.inbox('engineer', unread_only=False)), 1)
        self.assertIsNotNone(self.store.message(mid)['read_at'])
        self.store.mark_read(mid, 'engineer')  # idempotent
        self.assertEqual(self.store.message(mid)['read_at'], START)

    def test_sha256_is_stable(self):
        self.assertEqual(sha256('abc'), sha256('abc'))
        self.assertNotEqual(sha256('abc'), sha256('abd'))


class WorkPermissionTests(StoreTest):
    def setUp(self):
        super().setUp()
        self.team()
        self.mid = self.store.send('planner', 'engineer', 'Build the parser.', title='Parser', due_seconds=3600)

    def test_only_the_assigned_agent_can_accept_and_return(self):
        with self.assertRaises(ValueError):
            self.store.accept(self.mid, 'planner')
        with self.assertRaises(ValueError):
            self.store.return_work(self.mid, 'planner', 'Done.')
        with self.assertRaises(ValueError):
            self.store.return_work(self.mid, 'engineer', '   ')
        self.store.accept(self.mid, 'engineer')
        self.assertIsNotNone(self.store.work(self.mid)['accepted'])
        self.assertIsNotNone(self.store.message(self.mid)['read_at'])
        rid = self.store.return_work(self.mid, 'engineer', 'Parser merged.', evidence='12 tests')
        returned = self.store.message(rid)
        self.assertEqual((returned['sender'], returned['recipient']), ('engineer', 'planner'))
        self.assertIn('Parser merged', returned['body'])
        self.assertEqual(self.store.work_for_return(rid)['message_id'], self.mid)
        self.assertEqual(self.store.work(self.mid)['result']['disposition'], 'DONE')
        with self.assertRaises(ValueError):
            self.store.return_work(self.mid, 'engineer', 'Again.')

    def test_only_the_original_sender_can_close(self):
        self.store.return_work(self.mid, 'engineer', 'Done.')
        with self.assertRaises(ValueError):
            self.store.close_work(self.mid, 'engineer', 'accepted')
        with self.assertRaises(ValueError):
            self.store.close_work(self.mid, 'planner', 'shrug')
        self.store.close_work(self.mid, 'planner', 'accepted', note='Looks good.')
        closure = self.store.work(self.mid)['closure']
        self.assertEqual(closure['outcome'], 'accepted')
        self.assertEqual(closure['note'], 'Looks good.')
        self.assertEqual(set(CLOSE_OUTCOMES), {'accepted', 'revision', 'blocked'})

    def test_work_open_until_returned_or_closed_plain_message_until_read(self):
        self.assertTrue(self.store.work_open(self.mid))
        self.store.accept(self.mid, 'engineer')
        self.assertTrue(self.store.work_open(self.mid))  # reading work does not close it
        self.store.return_work(self.mid, 'engineer', 'Done.')
        self.assertFalse(self.store.work_open(self.mid))
        note = self.store.send('planner', 'engineer', 'FYI', intent='notification')
        self.assertTrue(self.store.work_open(note))
        self.store.mark_read(note, 'engineer')
        self.assertFalse(self.store.work_open(note))
        other = self.store.send('planner', 'reviewer', 'Please look.', title='Review')
        self.store.close_work(other, 'planner', 'blocked', note='Withdrawn.')
        self.assertFalse(self.store.work_open(other))

    def test_overdue_alerts_once_via_the_flag(self):
        self.assertEqual(self.store.overdue(), [])
        self.clock.advance(3600)
        due = self.store.overdue()
        self.assertEqual([w['message_id'] for w in due], [self.mid])
        self.store.mark_overdue_alerted(self.mid)
        self.assertEqual(self.store.overdue(), [])
        listed = self.store.work_list()
        self.assertEqual(listed[0]['message_id'], self.mid)
        self.assertEqual(listed[0]['sender'], 'planner')
        self.assertEqual(listed[0]['recipient'], 'engineer')


class HandoffRowTests(StoreTest):
    def test_update_validates_state_and_keeps_named_receipt_keys(self):
        self.team()
        mid = self.store.send('planner', 'engineer', 'Go.')
        at = self.clock()
        with self.store.connect() as db:
            db.execute(
                'INSERT INTO handoffs(id,sender,recipient,request_id,body_sha,target,status,created,updated) '
                'VALUES(?,?,?,?,?,?,?,?,?)',
                (mid, 'planner', 'engineer', 'req-1', sha256('Go.'),
                 '{"id":"engineer","provider":"demo","endpoint":"thread-engineer","cwd":""}',
                 'WAITING', at, at))
        self.assertEqual(set(STATES), set(PENDING + ACTIVE + TERMINAL))
        with self.assertRaises(ValueError):
            self.store.update(mid, 'NOPE', 'x')
        with self.assertRaises(ValueError):
            self.store.update('missing', 'WAITING', 'x')
        self.store.update(mid, 'ACCEPTED', 'ok', {'turnId': 't1', 'resends': 1, 'extra': 'a'})
        self.store.update(mid, 'RUNNING', 'working', {'turnId': 't1', 'seen': True}, keep=('resends',))
        row = self.store.handoff(mid)
        self.assertEqual(row['status'], 'RUNNING')
        self.assertEqual(row['receipt']['resends'], 1)
        self.assertEqual(row['receipt']['seen'], True)
        self.assertNotIn('extra', row['receipt'])
        self.assertEqual(self.store.handoffs(statuses=['RUNNING'])[0]['id'], mid)
        events = self.store.events(mid)
        self.assertEqual([e['status'] for e in events], ['ACCEPTED', 'RUNNING'])
        self.assertEqual(self.store.events(after=events[0]['sequence'])[0]['status'], 'RUNNING')


if __name__ == '__main__':
    unittest.main()
