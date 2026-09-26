"""The adapter registry and the two stable adapters: demo and command.

Real child processes here are only the Python interpreter running this suite,
and the only network is a throwaway HTTP server on 127.0.0.1.
"""
import collections
import contextlib
import http.server
import importlib
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

from agentbrain_handoffs import cards, transports
from agentbrain_handoffs.engine import Engine
from agentbrain_handoffs.store import Store
from agentbrain_handoffs.transport import NotAccepted, Router, default_router
from agentbrain_handoffs.transports import command as command_module
from agentbrain_handoffs.transports import demo as demo_module
from agentbrain_handoffs.transports.command import CommandTransport
from agentbrain_handoffs.transports.demo import SUMMARIES, DemoTransport, assignment_id

START = 1_800_000_000.0


class FakeClock:
    def __init__(self, now=START):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def wait_for(predicate, timeout=10.0):
    """Poll real time for something a real child process does."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError('Condition not met within %.1f s' % timeout)


def request_with(transport, outcome):
    """A request id the demo will simulate with the given outcome."""
    for i in range(1000):
        request_id = 'req-%s-%d' % (outcome, i)
        if transport.plan(request_id)[0] == outcome:
            return request_id
    raise AssertionError('No request id found for ' + outcome)


class TempDirTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)


# ---- registry ---------------------------------------------------------------------------
class RegistryTests(unittest.TestCase):
    def test_lists_every_provider(self):
        adapters = transports.available()
        self.assertEqual(set(adapters), {'demo', 'command', 'codex', 'claude-code'})
        self.assertIs(adapters['demo'], DemoTransport)
        self.assertIs(adapters['command'], CommandTransport)
        for factory in adapters.values():
            self.assertTrue(callable(factory))

    def test_default_router_registers_every_provider(self):
        router = default_router()
        self.assertEqual(set(router.adapters), {'demo', 'command', 'codex', 'claude-code'})
        self.assertIsInstance(router.adapters['demo'], DemoTransport)

    def test_a_broken_experimental_adapter_never_breaks_the_others(self):
        real_import = importlib.import_module

        def broken_codex(name, package=None):
            if name == '.codex':
                raise SyntaxError('simulated breakage')
            return real_import(name, package)

        with mock.patch.object(transports.importlib, 'import_module', side_effect=broken_codex):
            adapters = transports.available()
        self.assertIs(adapters['demo'], DemoTransport)
        self.assertIs(adapters['command'], CommandTransport)
        self.assertTrue(issubclass(adapters['codex'], transports.UnavailableTransport))
        self.assertNotIn('simulated breakage', getattr(adapters['claude-code'], 'reason', ''))

        stand_in = adapters['codex'](binary='ignored')
        self.assertIn('codex adapter could not be loaded', stand_in.reason)
        self.assertIn('simulated breakage', stand_in.reason)
        self.assertEqual(stand_in.activity({'id': 'a'})['turnStatus'], 'unknown')
        with self.assertRaises(RuntimeError):
            stand_in.prepare({'id': 'a'})
        with self.assertRaises(RuntimeError):
            stand_in.observe({'id': 'a'}, 'req', {})

    def test_stand_in_holds_deliveries_without_spending_an_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            clock = FakeClock()
            store = Store(Path(tmp) / 'h.sqlite3', clock=clock)
            store.register_agent('lead', 'Lead', provider='demo')
            store.register_agent('coder', 'Coder', provider='codex', endpoint='thread-1')
            stand_in = transports.unavailable('codex', ImportError('missing piece'))()
            engine = Engine(store, Router({'codex': stand_in, 'demo': DemoTransport(clock=clock)}), clock=clock)
            mid = store.send('lead', 'coder', 'Please fix the flaky test')
            engine.tick()
            row = store.handoff(mid)
            self.assertEqual(row['status'], 'UNAVAILABLE')
            self.assertEqual(row['attempts'], 0)


# ---- demo -------------------------------------------------------------------------------
class DemoPlanTests(unittest.TestCase):
    def test_plan_is_repeatable_and_roughly_80_10_10(self):
        demo = DemoTransport(seed=7)
        ids = ['req-%d' % i for i in range(4000)]
        self.assertEqual([demo.plan(i) for i in ids[:50]], [DemoTransport(seed=7).plan(i) for i in ids[:50]])
        self.assertNotEqual([demo.plan(i) for i in ids[:50]], [DemoTransport(seed=8).plan(i) for i in ids[:50]])
        counts = collections.Counter(demo.plan(i)[0] for i in ids)
        self.assertAlmostEqual(counts['finished'] / len(ids), 0.80, delta=0.03)
        self.assertAlmostEqual(counts['failed'] / len(ids), 0.10, delta=0.03)
        self.assertAlmostEqual(counts['stopped'] / len(ids), 0.10, delta=0.03)
        for i in ids[:200]:
            outcome, seconds, summary = demo.plan(i)
            self.assertTrue(4.0 <= seconds <= 15.0)
            self.assertIn(summary, SUMMARIES)

    def test_speed_divides_duration(self):
        slow, fast = DemoTransport(seed=3), DemoTransport(seed=3, speed=4)
        self.assertAlmostEqual(fast.plan('req-x')[1], slow.plan('req-x')[1] / 4)
        self.assertEqual(fast.plan('req-x')[0], slow.plan('req-x')[0])
        with self.assertRaises(ValueError):
            DemoTransport(speed=0)

    def test_plan_is_the_same_in_another_process(self):
        code = ('import json; from agentbrain_handoffs.transports.demo import DemoTransport; '
                'print(json.dumps([DemoTransport(seed=5).plan("req-%d" % i) for i in range(20)]))')
        env = dict(os.environ, PYTHONHASHSEED='12345', PYTHONPATH=os.pathsep.join(sys.path))
        out = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=30, check=True)
        here = [list(DemoTransport(seed=5).plan('req-%d' % i)) for i in range(20)]
        self.assertEqual(json.loads(out.stdout), here)


class DemoTurnTests(TempDirTest):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.demo = DemoTransport(clock=self.clock)
        self.agent = {'id': 'engineer', 'provider': 'demo', 'endpoint': '', 'settings': {}}

    def start(self, request_id, agent=None, demo=None):
        demo, agent = demo or self.demo, agent or self.agent
        with demo.prepare(agent) as prepared:
            return demo.start(prepared, 'HANDOFF ' + request_id + '\nExact sender: a\nExact recipient: b\nMessage: m', request_id)

    def test_an_agent_without_turns_is_idle(self):
        self.assertEqual(self.demo.activity(self.agent), {'turnStatus': 'finished', 'turnId': None})

    def test_finished_turn_runs_then_returns(self):
        request_id = request_with(self.demo, 'finished')
        receipt = self.start(request_id)
        self.assertEqual(receipt['clientUserMessageId'], request_id)
        self.assertTrue(receipt['turnId'])
        self.assertEqual(self.demo.activity(self.agent), {'turnStatus': 'open', 'turnId': receipt['turnId']})
        self.assertEqual(self.demo.observe(self.agent, request_id, receipt)['status'], 'RUNNING')

        self.clock.advance(self.demo.plan(request_id)[1] + 0.01)
        self.assertEqual(self.demo.activity(self.agent)['turnStatus'], 'finished')
        seen = self.demo.observe(self.agent, request_id, receipt)
        self.assertEqual(seen['status'], 'RETURNED')
        self.assertEqual(seen['receipt']['turnOutcome'], 'completed')
        self.assertEqual(seen['receipt']['turnId'], receipt['turnId'])

    def test_failed_turn_is_failed(self):
        request_id = request_with(self.demo, 'failed')
        receipt = self.start(request_id)
        self.clock.advance(16)
        seen = self.demo.observe(self.agent, request_id, receipt)
        self.assertEqual(seen['status'], 'FAILED')
        self.assertEqual(seen['receipt']['turnOutcome'], 'failed')
        self.assertEqual(self.demo.activity(self.agent)['turnStatus'], 'failed')

    def test_stopped_turn_is_failed_and_interrupted(self):
        request_id = request_with(self.demo, 'stopped')
        receipt = self.start(request_id)
        self.clock.advance(16)
        seen = self.demo.observe(self.agent, request_id, {})  # found by request id alone
        self.assertEqual(seen['status'], 'FAILED')
        self.assertEqual(seen['receipt']['turnOutcome'], 'interrupted')
        self.assertEqual(self.demo.activity(self.agent), {'turnStatus': 'stopped', 'turnId': receipt['turnId']})

    def test_busy_agent_refuses_a_second_turn_and_one_request_makes_one_turn(self):
        first = self.start('req-a')
        with self.assertRaises(NotAccepted):
            self.start('req-b')
        self.assertEqual(self.start('req-a'), first)
        other = dict(self.agent, id='writer')
        self.assertNotEqual(self.start('req-b', agent=other)['turnId'], first['turnId'])

    def test_one_agent_never_answers_for_another(self):
        receipt = self.start('req-a')
        other = dict(self.agent, id='writer')
        self.assertEqual(self.demo.activity(other)['turnStatus'], 'finished')
        self.assertIsNone(self.demo.observe(other, 'req-a', receipt)['status'])

    def test_state_file_is_shared_and_private(self):
        path = self.tmp / 'turns.json'
        one = DemoTransport(state_path=path, clock=self.clock)
        two = DemoTransport(state_path=path, clock=self.clock)
        receipt = self.start('req-a', demo=one)
        self.assertEqual(two.activity(self.agent), {'turnStatus': 'open', 'turnId': receipt['turnId']})
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(json.loads(path.read_text())['turns'][0]['requestId'], 'req-a')

    def test_another_process_sees_the_same_turns(self):
        path = self.tmp / 'turns.json'
        live = DemoTransport(state_path=path)  # real clock: the turn lasts at least 4 s
        receipt = self.start('req-a', demo=live)
        code = ('import json, sys; from agentbrain_handoffs.transports.demo import DemoTransport; '
                'print(json.dumps(DemoTransport(state_path=sys.argv[1]).activity({"id": "engineer"})))')
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
        out = subprocess.run([sys.executable, '-c', code, str(path)], env=env, capture_output=True, text=True,
                             timeout=30, check=True)
        self.assertEqual(json.loads(out.stdout), {'turnStatus': 'open', 'turnId': receipt['turnId']})

    def test_default_state_file_sits_next_to_the_database(self):
        store = Store(self.tmp / 'handoffs.sqlite3')
        self.assertEqual(DemoTransport(store=store).state_path, self.tmp / 'handoffs.sqlite3.demo-turns.json')
        self.assertIsNone(DemoTransport().state_path)

    def test_missing_turn_is_proven_absent_only_with_complete_history(self):
        persistent = DemoTransport(state_path=self.tmp / 'turns.json', clock=self.clock)
        seen = persistent.observe(self.agent, 'req-never', {})
        self.assertIsNone(seen['status'])
        self.assertEqual(seen['receipt']['historySearch'], {'exhausted': True, 'candidate': None, 'turnId': None})
        # In memory, a restart would lose history, so absence proves nothing.
        self.assertNotIn('historySearch', self.demo.observe(self.agent, 'req-never', {})['receipt'])

    def test_damaged_state_file_starts_over_without_claiming_absence(self):
        path = self.tmp / 'turns.json'
        path.write_text('{not json')
        demo = DemoTransport(state_path=path, clock=self.clock)
        self.assertEqual(demo.activity(self.agent)['turnStatus'], 'finished')
        self.start('req-a', demo=demo)
        self.assertNotIn('historySearch', demo.observe(self.agent, 'req-never', {})['receipt'])

    def test_old_turns_are_pruned_and_then_absence_is_not_claimed(self):
        demo = DemoTransport(state_path=self.tmp / 'turns.json', clock=self.clock)
        with mock.patch.object(demo_module, 'MAX_TURNS', 3):
            for i in range(5):
                self.start('req-%d' % i, demo=demo)
                self.clock.advance(16)
                demo.advance()
        state = json.loads((self.tmp / 'turns.json').read_text())
        self.assertEqual([t['requestId'] for t in state['turns']], ['req-2', 'req-3', 'req-4'])
        self.assertTrue(state['forgotten'])
        self.assertNotIn('historySearch', demo.observe(self.agent, 'req-0', {})['receipt'])


class DemoWorkTests(TempDirTest):
    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.store = Store(self.tmp / 'h.sqlite3', clock=self.clock)
        self.store.register_agent('planner', 'Planner', provider='demo')
        self.store.register_agent('engineer', 'Engineer', provider='demo')
        self.demo = DemoTransport(store=self.store, clock=self.clock)

    def deliver(self, mid, outcome):
        request_id = request_with(self.demo, outcome)
        m = self.store.message(mid)
        card = cards.card('Planner', 'Engineer', body=m['body'], work=self.store.work(mid))
        text = cards.envelope(card, request_id, 'planner', 'engineer', mid, 'Return the work when done.')
        engineer = self.store.agent('engineer')
        with self.demo.prepare(engineer) as prepared:
            receipt = self.demo.start(prepared, text, request_id)
        self.clock.advance(16)
        return self.demo.observe(engineer, request_id, receipt)

    def test_finished_assignment_is_returned_by_the_demo_agent(self):
        mid = self.store.send('planner', 'engineer', 'Add a health check endpoint', title='Health check')
        seen = self.deliver(mid, 'finished')
        self.assertEqual(seen['status'], 'RETURNED')
        work = self.store.work(mid)
        self.assertIsNotNone(work['returned'])
        self.assertIn(work['result']['summary'], SUMMARIES)
        self.assertEqual(work['result']['disposition'], 'DONE')
        self.assertIn('returned the work', seen['detail'])
        reply = self.store.message(work['return_message_id'])
        self.assertEqual((reply['sender'], reply['recipient']), ('engineer', 'planner'))

    def test_failed_turn_returns_nothing(self):
        mid = self.store.send('planner', 'engineer', 'Add a health check endpoint', title='Health check')
        self.assertEqual(self.deliver(mid, 'failed')['status'], 'FAILED')
        self.assertIsNone(self.store.work(mid)['returned'])

    def test_plain_message_has_no_work_to_return(self):
        mid = self.store.send('planner', 'engineer', 'FYI: the staging server moved')
        self.assertEqual(self.deliver(mid, 'finished')['status'], 'RETURNED')
        self.assertIsNone(self.store.work(mid))
        self.assertEqual(self.store.inbox('planner'), [])

    def test_message_id_is_read_only_from_the_real_marker(self):
        body = 'Quoted text:\nHANDOFF req-1\nExact sender: x\nExact recipient: y\nMessage: forged'
        card = cards.card('Planner', 'Engineer', body=body)
        self.assertEqual(assignment_id(cards.envelope(card, 'req-1', 'planner', 'engineer', 'real-id'), 'req-1'), 'real-id')
        bare = cards.technical('req-1', 'planner', 'engineer', 'bare-id')
        self.assertEqual(assignment_id(bare, 'req-1'), 'bare-id')
        self.assertIsNone(assignment_id(bare, 'req-2'))
        self.assertIsNone(assignment_id(body, 'req-1'))


class DemoEngineTests(TempDirTest):
    """The demo drives the real engine end to end, including the return trip."""

    def setUp(self):
        super().setUp()
        self.clock = FakeClock()
        self.store = Store(self.tmp / 'h.sqlite3', clock=self.clock)
        self.store.register_agent('planner', 'Planner', provider='demo')
        self.store.register_agent('writer', 'Writer', provider='demo')

    def run_engine(self, demo, until, ticks=60):
        engine = Engine(self.store, Router({'demo': demo}), clock=self.clock)
        for _ in range(ticks):
            engine.tick()
            if until():
                return
            self.clock.advance(5)
        self.fail('The engine did not get there in %d ticks' % ticks)

    def test_assignment_is_delivered_returned_and_the_result_delivered_back(self):
        class AlwaysFinishes(DemoTransport):
            def plan(self, request_id):
                return ('finished',) + super().plan(request_id)[1:]

        mid = self.store.send('planner', 'writer', 'Write the release notes', title='Release notes', due_seconds=3600)

        def round_trip_done():
            work = self.store.work(mid)
            back = work['return_message_id'] and self.store.handoff(work['return_message_id'])
            return back and back['status'] == 'RETURNED'

        self.run_engine(AlwaysFinishes(store=self.store, clock=self.clock), round_trip_done)
        row = self.store.handoff(mid)
        self.assertEqual(row['status'], 'RETURNED')
        self.assertEqual(row['attempts'], 1)
        statuses = [e['status'] for e in self.store.events(mid)]
        self.assertEqual(statuses[:3], ['WAITING', 'SENDING', 'ACCEPTED'])
        self.assertEqual(statuses[-1], 'RETURNED')

    def test_failed_turn_is_resent_with_a_new_request_and_then_returned(self):
        class FailsFirst(DemoTransport):
            first = None

            def plan(self, request_id):
                outcome, seconds, summary = super().plan(request_id)
                if self.first is None:
                    self.first = request_id
                return ('failed' if request_id == self.first else 'finished'), seconds, summary

        mid = self.store.send('planner', 'writer', 'Write the release notes', title='Release notes')
        demo = FailsFirst(store=self.store, clock=self.clock)
        self.run_engine(demo, lambda: self.store.handoff(mid)['status'] == 'RETURNED')
        row = self.store.handoff(mid)
        self.assertEqual(row['receipt']['resends'], 1)
        self.assertNotEqual(row['request_id'], demo.first)
        self.assertIsNotNone(self.store.work(mid)['returned'])


# ---- command: processes -----------------------------------------------------------------
AGENT_SCRIPT = r'''
import os, sys, time
text_file, request_id, endpoint, cwd, mode, code = sys.argv[1:7]
print('file:', text_file)
print('text:', open(text_file, encoding='utf-8').read().splitlines()[0])
print('request:', request_id, os.environ['HANDOFFS_REQUEST_ID'], os.environ['HANDOFFS_AGENT_ID'])
print('endpoint:', endpoint)
print('cwd:', cwd)
sys.stdout.flush()
if mode == 'wait':
    deadline = time.time() + 20
    while not os.path.exists(os.path.join(cwd, 'release')) and time.time() < deadline:
        time.sleep(0.02)
elif mode == 'sleep':
    time.sleep(60)
elif mode == 'many':
    for i in range(1, 31):
        print('line', i)
sys.exit(int(code))
'''


class CommandProcessTests(TempDirTest):
    def setUp(self):
        super().setUp()
        (self.tmp / 'agent.py').write_text(AGENT_SCRIPT)
        self.transport = CommandTransport()
        self.addCleanup(self.release)

    def release(self):
        (self.tmp / 'release').touch()

    def agent(self, mode='exit', code=0, **settings):
        argv = [sys.executable, str(self.tmp / 'agent.py'), '{text_file}', '{request_id}', '{endpoint}', '{cwd}',
                mode, str(code)]
        return {'id': 'builder', 'name': 'Builder', 'provider': 'command', 'endpoint': 'session-42',
                'cwd': str(self.tmp), 'settings': dict({'command': argv}, **settings)}

    def start(self, agent, request_id, text='Ship the fix\nwith details'):
        with self.transport.prepare(agent) as prepared:
            return self.transport.start(prepared, text, request_id)

    def settled(self, agent, request_id):
        seen = self.transport.observe(agent, request_id, {})
        return seen if seen['status'] != 'RUNNING' else None

    def test_runs_with_placeholders_and_reports_running_then_returned(self):
        agent = self.agent('wait')
        began = time.monotonic()
        receipt = self.start(agent, 'req-1')
        self.assertLess(time.monotonic() - began, 5)  # returns while the command still runs
        self.assertEqual((receipt['turnId'], receipt['clientUserMessageId']), ('req-1', 'req-1'))
        self.assertEqual(self.transport.activity(agent), {'turnStatus': 'open', 'turnId': 'req-1'})
        self.assertEqual(self.transport.observe(agent, 'req-1', receipt)['status'], 'RUNNING')

        self.release()
        seen = wait_for(lambda: self.settled(agent, 'req-1'))
        self.assertEqual(seen['status'], 'RETURNED')
        self.assertEqual(seen['receipt']['exitCode'], 0)
        tail = seen['receipt']['outputTail']
        self.assertIn('text: Ship the fix', tail)
        self.assertIn('request: req-1 req-1 builder', tail)
        self.assertIn('endpoint: session-42', tail)
        self.assertIn('cwd: ' + str(self.tmp), tail)
        text_file = tail[0][len('file: '):]
        self.assertFalse(os.path.exists(text_file), 'the handoff text file is removed after the run')
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'finished')

    def test_nonzero_exit_fails_with_the_code_and_last_20_lines(self):
        agent = self.agent('many', 3)
        self.start(agent, 'req-1')
        seen = wait_for(lambda: self.settled(agent, 'req-1'))
        self.assertEqual(seen['status'], 'FAILED')
        self.assertEqual(seen['receipt']['exitCode'], 3)
        self.assertEqual(seen['receipt']['turnOutcome'], 'failed')
        self.assertEqual(seen['receipt']['outputTail'], ['line %d' % i for i in range(11, 31)])
        self.assertIn('code 3', seen['detail'])
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'failed')

    def test_timeout_stops_the_command(self):
        agent = self.agent('sleep', timeout=0.5)
        self.start(agent, 'req-1')
        seen = wait_for(lambda: self.settled(agent, 'req-1'))
        self.assertEqual(seen['status'], 'FAILED')
        self.assertTrue(seen['receipt']['timedOut'])
        self.assertEqual(seen['receipt']['turnOutcome'], 'interrupted')
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'stopped')

    def test_a_command_that_cannot_start_is_not_accepted(self):
        agent = self.agent()
        agent['settings']['command'] = [str(self.tmp / 'no-such-agent')]
        with self.assertRaises(NotAccepted):
            self.start(agent, 'req-1')
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'finished')
        self.assertIsNone(self.transport.observe(agent, 'req-1', {})['status'])

    def test_busy_agent_refuses_and_one_request_runs_once(self):
        agent = self.agent('wait')
        first = self.start(agent, 'req-1')
        with self.assertRaises(NotAccepted):
            self.start(agent, 'req-2')
        self.assertEqual(self.start(agent, 'req-1')['pid'], first['pid'])
        self.release()
        wait_for(lambda: self.settled(agent, 'req-1'))

    def test_unknown_run_is_reported_as_unknown_not_guessed(self):
        seen = self.transport.observe(self.agent(), 'req-from-before-a-restart', {'turnId': 'req-from-before-a-restart'})
        self.assertIsNone(seen['status'])
        self.assertNotIn('historySearch', seen['receipt'])

    def test_engine_delivers_to_a_command_agent(self):
        clock = FakeClock()
        store = Store(self.tmp / 'h.sqlite3', clock=clock)
        store.register_agent('lead', 'Lead', provider='demo')
        worker = self.agent('exit', 0)
        store.register_agent('builder', 'Builder', provider='command', endpoint='session-42', cwd=str(self.tmp),
                             settings=worker['settings'])
        engine = Engine(store, Router({'command': self.transport, 'demo': DemoTransport(clock=clock)}), clock=clock)
        mid = store.send('lead', 'builder', 'Rebuild the index', title='Rebuild index')

        def returned():
            engine.tick()
            clock.advance(11)
            return store.handoff(mid)['status'] == 'RETURNED'

        wait_for(returned)
        row = store.handoff(mid)
        self.assertEqual(row['attempts'], 1)
        self.assertEqual(row['receipt']['exitCode'], 0)
        self.assertTrue(any(line.startswith('text: ') for line in row['receipt']['outputTail']))
        self.assertIn('ACCEPTED', [e['status'] for e in store.events(mid)])


SRC = str(Path(__file__).resolve().parents[1] / 'src')

IDENTITY_SCRIPT = r'''
import os
print('agent:', os.environ.get('HANDOFFS_AGENT'))
print('db:', os.environ.get('HANDOFFS_DB'))
'''

# A command agent that does what the card tells it: run the `handoffs return ...` line.
CARD_FOLLOWER = r'''
import re, shlex, sys
sys.path.insert(0, %r)
text = open(sys.argv[1], encoding='utf-8').read()
line = re.search(r'`(handoffs return [^`]+)`', text).group(1)
from agentbrain_handoffs.cli import main
sys.exit(main(shlex.split(line.replace('"..."', '"Rebuilt the index."'))[1:]))
''' % SRC


class CommandRunRecordTests(TempDirTest):
    """Runs are supervised and recorded outside the engine, so any engine can observe them."""

    def setUp(self):
        super().setUp()
        (self.tmp / 'agent.py').write_text(AGENT_SCRIPT)
        self.store = Store(self.tmp / 'h.sqlite3')
        self.addCleanup(lambda: (self.tmp / 'release').touch())

    def agent(self, mode='exit', code=0, script='agent.py', **settings):
        argv = [sys.executable, str(self.tmp / script), '{text_file}', '{request_id}', '{endpoint}', '{cwd}',
                mode, str(code)]
        return {'id': 'builder', 'name': 'Builder', 'provider': 'command', 'endpoint': 'session-42',
                'cwd': str(self.tmp), 'settings': dict({'command': argv}, **settings)}

    @staticmethod
    def start(transport, agent, request_id, text='Ship the fix'):
        with transport.prepare(agent) as prepared:
            return transport.start(prepared, text, request_id)

    @staticmethod
    def settled(transport, agent, request_id, receipt=None):
        seen = transport.observe(agent, request_id, receipt or {})
        return seen if seen['status'] != 'RUNNING' else None

    def test_a_run_outlives_the_adapter_that_started_it(self):
        agent = self.agent('wait')
        receipt = self.start(CommandTransport(store=self.store), agent, 'req-1')
        restarted = CommandTransport(store=self.store)  # a new engine process on the same database
        self.assertEqual(restarted.activity(agent), {'turnStatus': 'open', 'turnId': 'req-1'})
        self.assertEqual(restarted.observe(agent, 'req-1', receipt)['status'], 'RUNNING')
        with self.assertRaises(NotAccepted):
            self.start(restarted, agent, 'req-2')  # still never two runs at once
        (self.tmp / 'release').touch()
        seen = wait_for(lambda: self.settled(restarted, agent, 'req-1'))
        self.assertEqual((seen['status'], seen['receipt']['exitCode']), ('RETURNED', 0))
        self.assertIn('request: req-1 req-1 builder', seen['receipt']['outputTail'])
        self.assertEqual(restarted.activity(agent)['turnStatus'], 'finished')

    def test_an_adapter_without_the_store_finds_the_run_through_its_receipt(self):
        agent = self.agent('exit')
        receipt = self.start(CommandTransport(), agent, 'req-1')
        other = CommandTransport()
        seen = wait_for(lambda: self.settled(other, agent, 'req-1', receipt))
        self.assertEqual(seen['status'], 'RETURNED')
        self.assertIsNone(other.observe(agent, 'req-1', {})['status'])
        forged = dict(receipt, runState=str(self.tmp / 'elsewhere.json'))
        self.assertIsNone(other.observe(agent, 'req-1', forged)['status'])
        self.assertIsNone(other.observe(dict(agent, id='someone-else'), 'req-1', receipt)['status'])

    def test_a_run_whose_runner_died_counts_as_interrupted_not_running(self):
        agent = self.agent('sleep')
        transport = CommandTransport(store=self.store)
        receipt = self.start(transport, agent, 'req-1')
        record = json.loads(Path(receipt['runState']).read_text())
        os.kill(record['runnerPid'], signal.SIGKILL)  # a reboot or an out-of-memory kill
        with contextlib.suppress(ProcessLookupError):
            os.killpg(receipt['pid'], signal.SIGKILL)
        seen = wait_for(lambda: self.settled(transport, agent, 'req-1'))
        self.assertEqual(seen['status'], 'FAILED')
        self.assertEqual(seen['receipt']['turnOutcome'], 'interrupted')
        self.assertIn('runner ended', seen['detail'])
        self.assertEqual(transport.activity(agent), {'turnStatus': 'stopped', 'turnId': 'req-1'})
        self.start(transport, agent, 'req-2')  # the agent is free again

    def test_the_turn_acts_as_the_recipient_on_the_engines_database(self):
        (self.tmp / 'identity.py').write_text(IDENTITY_SCRIPT)
        agent = self.agent(script='identity.py')
        with mock.patch.dict(os.environ, {'HANDOFFS_AGENT': 'planner', 'HANDOFFS_DB': '/elsewhere.sqlite3'}):
            self.start(CommandTransport(store=self.store), agent, 'req-1')
        seen = wait_for(lambda: self.settled(CommandTransport(store=self.store), agent, 'req-1'))
        self.assertIn('agent: builder', seen['receipt']['outputTail'])
        self.assertIn('db: ' + str(self.store.path.absolute()), seen['receipt']['outputTail'])

    def test_a_command_agent_can_follow_its_card_and_return_the_work(self):
        (self.tmp / 'follower.py').write_text(CARD_FOLLOWER)
        self.store.register_agent('lead', 'Lead', provider='demo')
        worker = self.agent(script='follower.py')
        self.store.register_agent('builder', 'Builder', provider='command', cwd=str(self.tmp),
                                  settings=worker['settings'])
        clock = FakeClock()
        engine = Engine(self.store, Router({'command': CommandTransport(store=self.store),
                                            'demo': DemoTransport(clock=clock)}), clock=clock)
        mid = self.store.send('lead', 'builder', 'Rebuild the index', title='Rebuild index')
        env = {k: v for k, v in os.environ.items() if k != 'HANDOFFS_DB'}
        env['HANDOFFS_AGENT'] = 'lead'  # whoever started the engine; the turn must not act as them

        def returned():
            engine.tick()
            clock.advance(11)
            return self.store.handoff(mid)['status'] in ('RETURNED', 'FAILED')

        with mock.patch.dict(os.environ, env, clear=True):
            wait_for(returned)
        row = self.store.handoff(mid)
        self.assertEqual((row['status'], row['receipt']['exitCode']), ('RETURNED', 0), row['receipt'].get('outputTail'))
        work = self.store.work(mid)
        self.assertEqual(work['result']['summary'], 'Rebuilt the index.')
        self.assertEqual(self.store.message(work['return_message_id'])['sender'], 'builder')


class CommandSettingsTests(unittest.TestCase):
    def check(self, settings):
        return command_module.settings({'id': 'a', 'settings': settings})

    def test_defaults_and_string_commands(self):
        config = self.check({'command': 'python3 agent.py "{text_file}"'})
        self.assertEqual(config['command'], ['python3', 'agent.py', '{text_file}'])
        self.assertEqual(config['timeout'], 3600)
        self.assertIsNone(config['url'])

    def test_bad_settings_explain_themselves(self):
        for bad in ({}, {'command': []}, {'command': 5}, {'command': ['ok', '']}, {'url': 'ftp://example.test/x'},
                    {'command': ['a'], 'timeout': 0}, {'command': ['a'], 'timeout': True},
                    {'url': 'http://127.0.0.1:1/', 'status_url': 'file:///etc/hosts'}):
            with self.subTest(bad=bad), self.assertRaises(ValueError) as caught:
                self.check(bad)
            self.assertTrue(str(caught.exception).endswith('.'))

    def test_prepare_raises_before_anything_runs(self):
        transport = CommandTransport()
        with self.assertRaises(ValueError):
            with transport.prepare({'id': 'a', 'settings': {}}):
                pass
        self.assertEqual(transport.activity({'id': 'a', 'settings': {}})['turnStatus'], 'finished')

    def test_only_known_placeholders_are_filled(self):
        values = {'text_file': '/tmp/t', 'request_id': 'r1', 'endpoint': 'e', 'cwd': 'c'}
        self.assertEqual(command_module.fill('{"id": "{request_id}"} {other}', values), '{"id": "r1"} {other}')


# ---- command: HTTP ----------------------------------------------------------------------
class AgentEndpoint:
    """A tiny local agent service: records POSTs and answers status GETs."""

    def __init__(self):
        self.posts = []
        self.gets = []
        self.post_code = 202
        self.statuses = {}
        endpoint = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, payload=None, headers=()):
                body = json.dumps(payload or {}).encode()
                self.send_response(code)
                for name, value in headers:
                    self.send_header(name, value)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                length = int(self.headers.get('Content-Length') or 0)
                endpoint.posts.append((self.path, json.loads(self.rfile.read(length))))
                if self.path == '/moved':
                    return self.reply(302, headers=[('Location', '/agent')])
                self.reply(endpoint.post_code)

            def do_GET(self):
                endpoint.gets.append(self.path)
                parts = urllib.parse.urlsplit(self.path)
                request_id = (urllib.parse.parse_qs(parts.query).get('request_id') or [parts.path.rsplit('/', 1)[-1]])[0]
                if request_id in endpoint.statuses:
                    return self.reply(200, {'status': endpoint.statuses[request_id]})
                self.reply(404, {'error': 'unknown request'})

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.base = 'http://127.0.0.1:%d' % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class CommandHttpTests(unittest.TestCase):
    def setUp(self):
        self.endpoint = AgentEndpoint()
        self.addCleanup(self.endpoint.close)
        self.transport = CommandTransport()

    def agent(self, **settings):
        return {'id': 'service-agent', 'name': 'Service', 'provider': 'command', 'endpoint': 'queue-1', 'cwd': '',
                'settings': dict({'url': self.endpoint.base + '/agent', 'secret_note': 'stays local'}, **settings)}

    def start(self, agent, request_id='req-1'):
        with self.transport.prepare(agent) as prepared:
            return self.transport.start(prepared, 'Do the thing', request_id)

    def test_accepted_post_without_status_url_counts_as_returned(self):
        agent = self.agent()
        receipt = self.start(agent)
        self.assertEqual(receipt, {'turnId': 'req-1', 'clientUserMessageId': 'req-1', 'httpStatus': 202})
        path, payload = self.endpoint.posts[0]
        self.assertEqual(path, '/agent')
        self.assertEqual((payload['request_id'], payload['text'], payload['agent']['id']), ('req-1', 'Do the thing', 'service-agent'))
        self.assertNotIn('settings', payload['agent'])
        self.assertEqual(self.transport.observe(agent, 'req-1', receipt)['status'], 'RETURNED')
        self.assertIsNone(self.transport.observe(agent, 'req-1', {})['status'])  # no accepted receipt, no guess

    def test_status_url_is_followed(self):
        agent = self.agent(status_url=self.endpoint.base + '/status/{request_id}')
        receipt = self.start(agent)
        self.endpoint.statuses['req-1'] = 'running'
        self.assertEqual(self.transport.observe(agent, 'req-1', receipt)['status'], 'RUNNING')
        self.assertEqual(self.transport.activity(agent), {'turnStatus': 'open', 'turnId': 'req-1'})
        self.endpoint.statuses['req-1'] = 'returned'
        seen = self.transport.observe(agent, 'req-1', receipt)
        self.assertEqual((seen['status'], seen['receipt']['turnOutcome']), ('RETURNED', 'completed'))
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'finished')
        self.endpoint.statuses['req-1'] = 'stopped'
        seen = self.transport.observe(agent, 'req-1', receipt)
        self.assertEqual((seen['status'], seen['receipt']['turnOutcome']), ('FAILED', 'interrupted'))
        self.assertIn('/status/req-1', self.endpoint.gets)

    def test_status_url_without_placeholder_gets_a_query_parameter(self):
        agent = self.agent(status_url=self.endpoint.base + '/status')
        self.endpoint.statuses['req-9'] = 'failed'
        self.assertEqual(self.transport.observe(agent, 'req-9', {})['status'], 'FAILED')
        self.assertEqual(self.endpoint.gets, ['/status?request_id=req-9'])

    def test_unknown_request_at_the_status_url_is_proven_absent(self):
        agent = self.agent(status_url=self.endpoint.base + '/status/{request_id}')
        seen = self.transport.observe(agent, 'req-never', {})
        self.assertIsNone(seen['status'])
        self.assertEqual(seen['receipt']['historySearch'], {'exhausted': True, 'candidate': None, 'turnId': None})

    def test_unrecognized_status_is_not_guessed(self):
        agent = self.agent(status_url=self.endpoint.base + '/status/{request_id}')
        self.endpoint.statuses['req-1'] = 'thinking-hard'
        self.assertIsNone(self.transport.observe(agent, 'req-1', {})['status'])

    def test_refusals_and_unknown_answers(self):
        self.endpoint.post_code = 409
        with self.assertRaises(NotAccepted):
            self.start(self.agent())
        self.endpoint.post_code = 500
        with self.assertRaises(RuntimeError) as caught:
            self.start(self.agent(), 'req-2')
        self.assertNotIsInstance(caught.exception, NotAccepted)  # it may have been taken: never retried

    def test_redirects_are_refused_not_followed(self):
        with self.assertRaises(NotAccepted):
            self.start(self.agent(url=self.endpoint.base + '/moved'))
        self.assertEqual([path for path, _ in self.endpoint.posts], ['/moved'])
        self.assertEqual(self.endpoint.gets, [])

    def test_nothing_listening_is_not_accepted(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        with self.assertRaises(NotAccepted):
            self.start(self.agent(url='http://127.0.0.1:%d/agent' % port))


if __name__ == '__main__':
    unittest.main()
