"""Tests for the delivery engine: every guarantee in engine.py's docstring, proven with a
scripted fake agent app and a settable clock, so hours of delivery history run in
milliseconds and nothing ever talks to a real agent.
"""
import contextlib
import copy
import json
import os
import sys
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

# Run straight from a checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from agentbrain_handoffs import cards  # noqa: E402
from agentbrain_handoffs.engine import (ABSENT_SCANS, ABSENT_SECONDS, POLL_FAST, POLL_FAST_WINDOW,  # noqa: E402
                                        POLL_SLOW, RESEND_MAX_FAILED, RESEND_STOPPED_WAIT, SCAN_ROWS,
                                        SENDING_TIMEOUT, UNOBSERVED_LIMIT, Engine)
from agentbrain_handoffs.store import Store  # noqa: E402
from agentbrain_handoffs.transport import NotAccepted, Transport  # noqa: E402

START = 1_800_000_000.0
ABSENT = {'historySearch': {'exhausted': True, 'candidate': None, 'turnId': None}}


class Clock:
    """A wall clock the test moves by hand."""

    def __init__(self, now=START):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        return self.now


class FakeTransport(Transport):
    """A scripted agent app. The test chooses every answer; every call is recorded.

    - activity: a per-agent queue of answers ('open', a dict, or an exception to raise).
      The last answer repeats. Agents with no script report 'finished'.
    - start: a queue of answers (a dict to return, an exception to raise, a callable to
      run, or any other value to return as-is). When the queue is empty the app confirms
      the turn with a fresh turn id, like a healthy provider.
    - observe: answers keyed by request id, falling back to the '*' key, else nothing.
    """
    name = 'fake'
    needs_host = False

    def __init__(self):
        self.lock = threading.Lock()
        self.activities = {}
        self.start_script = []
        self.observations = {}
        self.on_prepare = None
        self.prepares = []
        self.starts = []
        self.observed = []

    def script_activity(self, agent_id, *answers):
        self.activities[agent_id] = list(answers)

    def activity(self, agent):
        with self.lock:
            queue = self.activities.get(agent['id'])
            if not queue:
                return {'turnStatus': 'finished', 'turnId': None}
            answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(answer, BaseException):
            raise answer
        if isinstance(answer, str):
            return {'turnStatus': answer, 'turnId': None}
        return dict(answer)

    @contextlib.contextmanager
    def prepare(self, agent):
        with self.lock:
            self.prepares.append(agent['id'])
        if self.on_prepare:
            self.on_prepare(agent)
        yield {'agent': dict(agent)}

    def start(self, prepared, text, request_id):
        with self.lock:
            self.starts.append({'agent': prepared['agent'], 'text': text, 'request_id': request_id})
            number = len(self.starts)
            answer = self.start_script.pop(0) if self.start_script else None
        if answer is None:
            return {'turnId': 'turn-' + str(number), 'clientUserMessageId': request_id}
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer(prepared, text, request_id)
        return answer

    def observe(self, agent, request_id, receipt):
        with self.lock:
            self.observed.append(request_id)
            answer = self.observations.get(request_id, self.observations.get('*', {}))
        if isinstance(answer, BaseException):
            raise answer
        return copy.deepcopy(answer)

    def starts_for(self, message_id):
        """Every provider write that delivered this inbox message."""
        return [s for s in self.starts if '\nMessage: ' + message_id in s['text']]


class EngineTest(unittest.TestCase):
    """Three registered agents on one fake app, a fresh database and a frozen clock."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        self.db_path = self.dir / 'handoffs.sqlite3'
        self.clock = Clock()
        self.store = Store(self.db_path, clock=self.clock)
        self.fake = FakeTransport()
        self.engine = self.make_engine()
        for agent_id, name in (('planner', 'Planner'), ('engineer', 'Engineer'), ('reviewer', 'Reviewer')):
            self.store.register_agent(agent_id, name, provider='fake', endpoint='thread-' + agent_id)

    def make_engine(self, **options):
        """Another engine on the same database, as a second process would have."""
        store = Store(self.db_path, clock=self.clock)
        return Engine(store, options.pop('transport', self.fake), clock=self.clock, monotonic=self.clock, **options)

    def tick(self, advance=0, engine=None):
        self.clock.advance(advance)
        return (engine or self.engine).tick()

    def assign(self, body='Build the parser.', sender='planner', recipient='engineer', **options):
        return self.store.send(sender, recipient, body, **options)

    def deliver(self, **options):
        """Send one handoff and tick once; the fake app accepts it."""
        mid = self.assign(**options)
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')
        return mid

    def row(self, mid):
        return self.store.handoff(mid)

    def observe(self, mid, status=None, **receipt):
        """Script what the app reports for this handoff's current request id."""
        self.fake.observations[self.row(mid)['request_id']] = {'status': status, 'detail': None, 'receipt': receipt}

    def tamper(self, mid, body):
        with self.store.connect() as db:
            db.execute('UPDATE messages SET body=? WHERE id=?', (body, mid))

    def add_agents(self, count):
        ids = ['agent-' + str(n) for n in range(count)]
        for agent_id in ids:
            self.store.register_agent(agent_id, agent_id.title(), provider='fake', endpoint='thread-' + agent_id)
        return ids


class ExactRecipientTests(EngineTest):
    def test_delivers_one_turn_to_the_exact_recipient_only(self):
        self.store.register_agent('engineer-2', 'Engineer', provider='fake', endpoint='thread-engineer-2')
        mid = self.deliver()
        self.assertEqual(self.fake.prepares, ['engineer'])
        self.assertEqual(len(self.fake.starts), 1)
        start = self.fake.starts[0]
        self.assertEqual((start['agent']['id'], start['agent']['endpoint']), ('engineer', 'thread-engineer'))
        self.assertIn('\nExact recipient: engineer\n', start['text'])
        row = self.row(mid)
        self.assertEqual(row['target'], {'id': 'engineer', 'provider': 'fake', 'endpoint': 'thread-engineer', 'cwd': ''})
        self.assertEqual(row['attempts'], 1)
        self.assertEqual(row['receipt']['turnId'], 'turn-1')
        self.assertEqual(row['receipt']['clientUserMessageId'], row['request_id'])

    def test_changed_endpoint_is_unavailable_and_no_substitute_is_chosen(self):
        mid = self.assign()
        self.engine.enroll_new()
        self.store.register_agent('engineer', 'Engineer', provider='fake', endpoint='thread-moved')
        self.store.register_agent('stand-in', 'Engineer', provider='fake', endpoint='thread-engineer')
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'UNAVAILABLE')
        self.tick(120)
        self.assertEqual(self.row(mid)['status'], 'UNAVAILABLE')
        self.assertEqual(self.fake.starts, [])
        # Once the exact endpoint resolves again, the original delivery goes ahead.
        self.store.register_agent('engineer', 'Engineer', provider='fake', endpoint='thread-engineer')
        self.tick(60)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')
        self.assertEqual([s['agent']['id'] for s in self.fake.starts], ['engineer'])

    def test_changed_provider_or_removed_recipient_is_unavailable(self):
        moved = self.assign()
        removed = self.assign(recipient='reviewer', body='Review the parser.')
        self.engine.enroll_new()
        self.store.register_agent('engineer', 'Engineer', provider='another-app', endpoint='thread-engineer')
        self.store.remove_agent('reviewer')
        self.tick()
        self.assertEqual(self.row(moved)['status'], 'UNAVAILABLE')
        self.assertEqual(self.row(removed)['status'], 'UNAVAILABLE')
        self.assertEqual(self.fake.starts, [])


class EnvelopeTests(EngineTest):
    def test_envelope_is_a_readable_card_then_the_exact_marker(self):
        mid = self.deliver(body='Build the parser.\nKeep it small.', title='Parser', due_seconds=3600)
        text = self.fake.starts[0]['text']
        request_id = self.row(mid)['request_id']
        self.assertTrue(text.startswith(cards.CARD_MARK + '**Parser** — Action requested\n'))
        self.assertIn('\nFrom Planner → Engineer\n', text)
        self.assertIn('\nDue: ', text)
        self.assertIn('\n> Build the parser.\n> Keep it small.\n', text)
        self.assertIn(cards.DETAILS + 'HANDOFF ' + request_id + '\n', text)
        self.assertIn('\nMessage: ' + mid, text)
        self.assertIn('handoffs return ' + mid, text)
        self.assertIn('`handoffs return ' + mid + ' --as engineer --summary', text)
        self.assertTrue(cards.marker_matches(text, request_id))
        self.assertFalse(cards.marker_matches(text, str(uuid.uuid4())))
        self.assertFalse(cards.marker_matches('> ' + text, request_id))

    def test_a_quoted_copy_of_a_delivery_never_matches_its_marker(self):
        first = self.deliver()
        forwarded = self.deliver(recipient='reviewer', body=self.fake.starts[0]['text'])
        text = self.fake.starts_for(forwarded)[0]['text']
        old_request = self.row(first)['request_id']
        self.assertIn('HANDOFF ' + old_request, text)  # present, but only inside the quote
        self.assertFalse(cards.marker_matches(text, old_request))
        self.assertTrue(cards.marker_matches(text, self.row(forwarded)['request_id']))

    def test_returned_work_goes_back_to_the_sender_as_a_return_card(self):
        mid = self.deliver(title='Parser')
        back = self.store.return_work(mid, 'engineer', 'Parser merged with tests.', evidence='12 tests pass')
        self.tick(10)
        row = self.row(back)
        self.assertEqual((row['sender'], row['recipient'], row['status']), ('engineer', 'planner', 'ACCEPTED'))
        [start] = self.fake.starts_for(back)
        self.assertEqual(start['agent']['id'], 'planner')
        self.assertIn('**Parser** — Review needed', start['text'])
        self.assertIn('\nSummary: Parser merged with tests.\n', start['text'])
        self.assertIn('handoffs close ' + mid, start['text'])
        self.assertIn('`handoffs close ' + mid + ' --as planner accepted|revision|blocked`', start['text'])
        self.assertTrue(cards.marker_matches(start['text'], row['request_id']))


class OneWritePerAttemptTests(EngineTest):
    def test_lost_reply_goes_uncertain_and_is_never_resent(self):
        self.fake.start_script = [TimeoutError('the connection closed before the reply')]
        mid = self.assign()
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        for _ in range(30):  # two and a half hours of reconciliation
            self.tick(300)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('UNCERTAIN', 1))
        self.assertEqual(len(self.fake.starts), 1)
        self.assertIn(row['request_id'], self.fake.observed)

    def test_reply_without_an_exact_receipt_goes_uncertain(self):
        replies = ({}, {'turnId': 'turn-x'}, {'turnId': 'turn-x', 'clientUserMessageId': 'another-request'})
        agents = self.add_agents(len(replies))
        self.fake.start_script = list(replies)
        mids = [self.assign(recipient=agent_id) for agent_id in agents]
        self.tick()
        for mid, reply in zip(mids, replies):
            with self.subTest(reply=reply):
                self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        self.tick(3600)
        self.assertEqual(len(self.fake.starts), len(replies))

    def test_uncertain_delivery_is_reconciled_by_observation(self):
        self.fake.start_script = [TimeoutError('no reply')]
        mid = self.assign()
        self.tick()
        self.observe(mid, 'RUNNING', turnId='turn-found')
        self.tick(10)
        row = self.row(mid)
        self.assertEqual(row['status'], 'RUNNING')
        self.assertEqual(row['receipt']['turnId'], 'turn-found')
        self.observe(mid, 'RETURNED')
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'RETURNED')
        self.assertEqual(len(self.fake.starts), 1)

    def test_crashed_send_turns_uncertain_after_a_minute_and_is_never_resent(self):
        mid = self.assign()
        self.engine.enroll_new()
        # The state a crash between reservation and reply leaves behind.
        self.assertTrue(self.engine.reserve(self.row(mid), retry=False))
        for _ in range(3):  # routine re-checks inside the first minute
            self.tick(15)
            self.assertEqual(self.row(mid)['status'], 'SENDING')
        self.tick(SENDING_TIMEOUT - 30)
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        for _ in range(10):
            self.tick(300)
        self.assertEqual(self.fake.starts, [])

    def test_second_engine_skips_while_the_first_is_ticking(self):
        entered, release = threading.Event(), threading.Event()

        def slow_start(prepared, text, request_id):
            entered.set()
            release.wait(5)
            return {'turnId': 'turn-slow', 'clientUserMessageId': request_id}

        self.fake.start_script = [slow_start]
        other = self.make_engine()
        mid = self.assign()
        worker = threading.Thread(target=self.engine.tick)
        worker.start()
        try:
            self.assertTrue(entered.wait(5))
            self.assertEqual(other.tick(), {'skipped': 'another engine is running'})
        finally:
            release.set()
            worker.join(5)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('ACCEPTED', 1))
        self.assertEqual(len(self.fake.starts), 1)

    def run_together(self, *jobs):
        """Run jobs on threads at once and re-raise the first error."""
        errors = []

        def guard(job):
            try:
                job()
            except BaseException as error:  # surfaced below
                errors.append(error)

        threads = [threading.Thread(target=guard, args=(job,)) for job in jobs]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        if errors:
            raise errors[0]

    def meet_in_prepare(self, parties=2):
        """Hold every engine inside prepare() until all have arrived, then release them together."""
        barrier = threading.Barrier(parties, timeout=5)
        met = []

        def wait(agent):
            barrier.wait()
            met.append(agent['id'])

        self.fake.on_prepare = wait
        return met

    def test_reservation_is_atomic_even_without_the_process_lock(self):
        one, two = self.make_engine(), self.make_engine()
        two.lock_path = self.dir / 'another.lock'  # as if file locking were unavailable
        met = self.meet_in_prepare()
        mid = self.assign()
        self.run_together(one.tick, two.tick)
        self.assertEqual(met, ['engineer', 'engineer'])  # both engines raced to the send boundary
        self.assertEqual(len(self.fake.starts), 1)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('ACCEPTED', 1))
        self.assertEqual([e['status'] for e in self.store.events(mid)].count('SENDING'), 1)

    def test_two_engines_never_put_two_deliveries_in_flight_to_one_recipient(self):
        one, two = self.make_engine(), self.make_engine()
        first = self.assign(body='First task.')
        self.clock.advance(1)
        second = self.assign(body='Second task.')
        self.engine.enroll_new()
        met = self.meet_in_prepare()
        self.run_together(lambda: one.step(self.row(first)), lambda: two.step(self.row(second)))
        self.assertEqual(len(met), 2)
        self.assertEqual(len(self.fake.starts), 1)
        rows = sorted((self.row(first), self.row(second)), key=lambda r: r['status'])
        self.assertEqual([r['status'] for r in rows], ['ACCEPTED', 'BUSY'])
        self.assertTrue(rows[1]['detail'].startswith('Waiting for the earlier delivery ' + rows[0]['id']))
        self.assertEqual(rows[1]['attempts'], 0)


class NeverInterruptTests(EngineTest):
    def test_busy_recipient_keeps_its_turn(self):
        self.fake.script_activity('engineer', {'turnStatus': 'open', 'turnId': 'their-own-turn'})
        mid = self.assign()
        self.tick()
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'BUSY')
        self.assertEqual((self.fake.prepares, self.fake.starts), ([], []))
        self.fake.script_activity('engineer', 'finished')
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_unverifiable_activity_sends_nothing(self):
        answers = ('unknown', RuntimeError('the agent app is not running'), {})
        agents = self.add_agents(len(answers))
        for agent_id, answer in zip(agents, answers):
            self.fake.script_activity(agent_id, answer)
        mids = [self.assign(recipient=agent_id) for agent_id in agents]
        self.tick()
        for mid in mids:
            self.assertEqual(self.row(mid)['status'], 'UNAVAILABLE')
        self.assertEqual(self.fake.starts, [])

    def test_recipient_that_turns_busy_during_preparation_is_not_interrupted(self):
        self.fake.script_activity('engineer', 'finished', 'open')
        mid = self.assign()
        self.tick()
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('BUSY', 0))
        self.assertEqual(self.fake.prepares, ['engineer'])
        self.assertEqual(self.fake.starts, [])

    def test_only_one_delivery_is_in_flight_per_recipient(self):
        # The app reports 'finished' throughout, so only the engine's own slot keeps order.
        first = self.assign(body='First task.')
        second = self.assign(body='Second task.')
        self.tick()
        self.assertEqual(self.row(first)['status'], 'ACCEPTED')
        self.assertEqual(self.row(second)['status'], 'BUSY')
        self.assertTrue(self.row(second)['detail'].startswith('Waiting for the earlier delivery ' + first))
        self.assertEqual(len(self.fake.starts), 1)
        self.observe(first, 'RETURNED')
        self.tick(10)
        self.assertEqual(self.row(first)['status'], 'RETURNED')
        self.assertEqual(self.row(second)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts), 2)

    def test_failure_report_while_the_turn_still_runs_is_not_final(self):
        mid = self.deliver()
        self.fake.script_activity('engineer', {'turnStatus': 'open', 'turnId': 'turn-1'})
        self.observe(mid, 'FAILED')
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'RUNNING')
        self.tick(10)
        self.assertEqual(len(self.fake.starts), 1)


class OwnerRejectedTests(EngineTest):
    def test_rejected_turn_is_retried_exactly_once(self):
        self.fake.start_script = [NotAccepted('the thread has another active writer')]
        mid = self.assign()
        self.tick()
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('OWNER_REJECTED', 1))
        self.assertIs(row['receipt']['rejectedBeforeAcceptance'], True)
        self.tick(15)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('ACCEPTED', 2))
        self.assertEqual([s['request_id'] for s in self.fake.starts], [row['request_id']] * 2)

    def test_a_second_rejection_is_not_retried_again(self):
        self.fake.start_script = [NotAccepted('busy'), NotAccepted('still busy')]
        mid = self.assign(title='Parser')
        self.tick()
        self.tick(15)
        row = self.row(mid)
        # Two explicit refusals prove no turn exists: final, not an in-flight UNCERTAIN.
        self.assertEqual((row['status'], row['attempts']), ('FAILED', 2))
        self.assertEqual(row['receipt']['resendDecision'], 'final')
        self.assertIn('Not retried further', row['detail'])
        self.assertIn('not delivered', row['detail'])
        for _ in range(20):
            self.tick(300)
        self.assertEqual(len(self.fake.starts), 2)

    def test_a_second_rejection_does_not_block_the_recipient(self):
        self.fake.start_script = [NotAccepted('refused'), NotAccepted('refused again')]
        first = self.assign(body='First task.')
        self.tick()
        self.tick(15)
        self.assertEqual(self.row(first)['status'], 'FAILED')
        later = self.assign(body='Second task.')
        self.tick(10)
        self.assertEqual(self.row(later)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(later)), 1)


class ResendTests(EngineTest):
    def fail(self, mid, **receipt):
        """The delivered turn fails; one tick records it, the next reviews it."""
        self.observe(mid, 'FAILED', **receipt)
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'FAILED')
        self.tick(10)

    def test_errored_turn_is_resent_twice_each_with_a_new_request_id(self):
        mid = self.deliver(title='Parser')
        request_ids = [self.row(mid)['request_id']]
        for number in range(1, RESEND_MAX_FAILED + 1):
            self.fail(mid)
            row = self.row(mid)
            self.assertEqual(row['status'], 'ACCEPTED')
            self.assertEqual((row['receipt']['resends'], row['receipt']['resentAfter']), (number, 'failed'))
            request_ids.append(row['request_id'])
        self.fail(mid)
        row = self.row(mid)
        self.assertEqual((row['status'], row['receipt']['resendDecision']), ('FAILED', 'final'))
        self.assertIn('resend limit reached', row['detail'])
        self.assertEqual(len(set(request_ids)), 3)
        self.assertEqual([s['request_id'] for s in self.fake.starts_for(mid)], request_ids)
        self.assertEqual([p['requestId'] for p in row['receipt']['previousRequests']], request_ids[:2])
        # A resend carries only its own marker; the earlier turn is never replayed.
        last = self.fake.starts_for(mid)[-1]['text']
        self.assertTrue(cards.marker_matches(last, request_ids[-1]))
        self.assertFalse(cards.marker_matches(last, request_ids[0]))
        for _ in range(10):
            self.tick(600)
        self.assertEqual(len(self.fake.starts_for(mid)), 3)

    def test_interrupted_turn_is_resent_once_after_ten_minutes(self):
        mid = self.deliver(title='Parser')
        self.observe(mid, 'FAILED', turnOutcome='interrupted')
        self.tick(10)
        self.tick(RESEND_STOPPED_WAIT - 1)
        self.assertEqual(self.row(mid)['status'], 'FAILED')
        self.assertEqual(len(self.fake.starts), 1)
        self.tick(1)
        row = self.row(mid)
        self.assertEqual((row['status'], row['receipt']['resentAfter']), ('ACCEPTED', 'interrupted'))
        self.assertEqual(len(self.fake.starts), 2)
        self.observe(mid, 'FAILED', turnOutcome='interrupted')
        self.tick(10)
        self.tick(RESEND_STOPPED_WAIT)
        row = self.row(mid)
        self.assertEqual((row['status'], row['receipt']['resendDecision']), ('FAILED', 'final'))
        self.assertEqual(len(self.fake.starts), 2)

    def test_a_stopped_turn_in_activity_counts_as_interrupted(self):
        mid = self.deliver()
        self.fake.script_activity('engineer', {'turnStatus': 'stopped', 'turnId': 'turn-1'})
        self.fail(mid)
        self.assertEqual(self.row(mid)['status'], 'FAILED')  # waiting out the ten minutes
        self.tick(RESEND_STOPPED_WAIT)
        self.assertEqual(self.row(mid)['receipt']['resentAfter'], 'interrupted')

    def test_no_resend_once_the_work_is_returned_closed_or_read(self):
        cases = ('returned', 'closed', 'read')
        for case, recipient in zip(cases, self.add_agents(len(cases))):
            with self.subTest(case=case):
                title = None if case == 'read' else 'Parser'  # 'read' closes a plain message, not work
                mid = self.deliver(recipient=recipient, title=title)
                if case == 'returned':
                    self.store.return_work(mid, recipient, 'Done in the meantime.')
                elif case == 'closed':
                    self.store.close_work(mid, 'planner', 'blocked', note='No longer needed.')
                else:
                    self.store.mark_read(mid, recipient)
                self.fail(mid)
                row = self.row(mid)
                self.assertEqual((row['status'], row['receipt']['resendDecision']), ('FAILED', 'final'))
                self.assertIn('already returned, closed or read', row['detail'])
                self.assertEqual(len(self.fake.starts_for(mid)), 1)

    def test_reading_work_does_not_close_it_so_it_is_still_resent(self):
        mid = self.deliver(title='Parser')
        self.store.accept(mid, 'engineer')
        self.fail(mid)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(mid)), 2)

    def test_a_failure_before_any_delivery_is_never_resent(self):
        mid = self.assign()
        self.engine.enroll_new()
        self.tamper(mid, 'Something else entirely.')
        self.tick()
        self.tick(10)
        row = self.row(mid)
        self.assertEqual((row['status'], row['receipt'].get('resendDecision')), ('FAILED', 'final'))
        self.assertEqual(self.fake.starts, [])


class ProvenAbsentTests(EngineTest):
    def lose_reply(self, **options):
        self.fake.start_script.append(TimeoutError('no reply'))
        mid = self.assign(**options)
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        return mid

    def test_released_after_three_complete_scans_spanning_an_hour(self):
        mid = self.lose_reply()
        self.fake.observations['*'] = {'status': None, 'receipt': ABSENT}
        self.tick(10)
        first_scan = self.clock.now
        self.assertEqual(self.row(mid)['receipt']['absentScans'], 1)
        for _ in range(ABSENT_SCANS - 1):
            self.tick(300)
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')  # three scans, but only ten minutes
        for _ in range(20):
            if self.row(mid)['status'] != 'UNCERTAIN':
                break
            self.tick(300)
        row = self.row(mid)
        self.assertEqual(row['status'], 'CANCELLED')
        self.assertGreaterEqual(self.clock.now - first_scan, ABSENT_SECONDS)
        self.assertGreaterEqual(row['receipt']['absentScans'], ABSENT_SCANS)
        self.assertIn('Nothing was resent', row['detail'])
        for _ in range(10):
            self.tick(600)
        self.assertEqual(self.row(mid)['status'], 'CANCELLED')
        self.assertEqual(len(self.fake.starts), 1)

    def test_an_hour_with_fewer_than_three_scans_is_not_enough(self):
        mid = self.lose_reply()
        self.fake.observations['*'] = {'status': None, 'receipt': ABSENT}
        self.tick(10)
        self.tick(ABSENT_SECONDS + 400)
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        self.tick(300)
        self.assertEqual(self.row(mid)['status'], 'CANCELLED')
        self.assertEqual(len(self.fake.starts), 1)

    def test_an_incomplete_or_ambiguous_search_never_releases(self):
        searches = ({'exhausted': False, 'candidate': None, 'turnId': None},
                    {'exhausted': True, 'candidate': 'turn-maybe', 'turnId': None},
                    {'exhausted': True, 'candidate': None, 'turnId': 'turn-maybe'})
        mids = [self.lose_reply(recipient=agent_id) for agent_id in self.add_agents(len(searches))]
        for mid, search in zip(mids, searches):
            self.fake.observations[self.row(mid)['request_id']] = {'status': None, 'receipt': {'historySearch': search}}
        for _ in range(60):  # five hours
            self.tick(300)
        for mid, search in zip(mids, searches):
            with self.subTest(search=search):
                self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')
        self.assertEqual(len(self.fake.starts), len(searches))


class OutageTests(EngineTest):
    def test_outage_hold_spends_no_attempt(self):
        reasons = ['The agent app reports a major outage; delivery is held.']
        engine = self.make_engine(outage=lambda: reasons[0] if reasons else None)
        mid = self.assign()
        self.tick(engine=engine)
        self.tick(60, engine=engine)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts'], row['detail']), ('WAITING', 0, reasons[0]))
        self.assertEqual((self.fake.prepares, self.fake.starts), ([], []))
        reasons.clear()
        self.tick(60, engine=engine)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('ACCEPTED', 1))


class DeadlineTests(EngineTest):
    def alerts(self, agent_id='planner'):
        return [m for m in self.store.inbox(agent_id, unread_only=False) if m['sender'] == 'service:deadline']

    def test_overdue_work_alerts_the_sender_once_without_waking_anyone(self):
        mid = self.deliver(title='Parser', due_seconds=60)
        self.tick(61)
        [alert] = self.alerts()
        self.assertEqual(alert['intent'], 'notification')
        self.assertIn('**Parser** — Needs attention', alert['body'])
        self.assertIsNone(self.store.handoff(alert['id']))  # never enrolled, so never a turn
        self.assertIsNotNone(self.store.work(mid)['overdue_alerted'])
        for _ in range(5):
            self.tick(600)
        self.assertEqual(len(self.alerts()), 1)
        self.assertEqual(self.fake.prepares, ['engineer'])

    def test_work_returned_in_time_is_never_overdue(self):
        mid = self.deliver(title='Parser', due_seconds=60)
        self.store.return_work(mid, 'engineer', 'Done early.')
        self.tick(3600)
        self.assertEqual(self.alerts(), [])

    def test_a_blocked_deadline_alert_does_not_stall_delivery(self):
        mid = self.deliver(title='Parser', due_seconds=60)
        self.store.set_connection('service:deadline', 'planner', allow=False)
        later = self.assign(recipient='reviewer', body='Review the parser.')
        report = self.tick(61)
        self.assertTrue(report['ok'])
        self.assertEqual(self.row(later)['status'], 'ACCEPTED')
        self.assertEqual(self.alerts(), [])
        self.assertIsNotNone(self.store.work(mid)['overdue_alerted'])


class DuplicateTests(EngineTest):
    def test_identical_handoff_while_the_first_is_in_flight_is_not_sent_twice(self):
        first = self.deliver(body='Run the migration.')
        second = self.assign(body='Run the migration.')
        self.tick(10)
        row = self.row(second)
        self.assertEqual(row['status'], 'DUPLICATE')
        self.assertIn(first, row['detail'])
        self.assertEqual(len(self.fake.starts), 1)

    def test_identical_handoff_after_the_first_finished_is_delivered(self):
        first = self.deliver(body='Run the migration.')
        self.assign(body='Run the migration.')
        self.tick(10)
        self.observe(first, 'RETURNED')
        self.tick(10)
        third = self.assign(body='Run the migration.')
        self.tick(10)
        self.assertEqual(self.row(third)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(third)), 1)

    def test_two_work_items_returned_with_the_same_summary_are_both_delivered(self):
        w1 = self.assign(body='Run the tests on branch A.', title='Run tests')
        w2 = self.assign(body='Run the tests on branch B.', title='Run tests')
        self.engine.enroll_new()
        r1 = self.store.return_work(w1, 'engineer', 'All tests pass.')
        r2 = self.store.return_work(w2, 'engineer', 'All tests pass.')
        self.assertEqual(self.store.message(r1)['body'], self.store.message(r2)['body'])
        self.engine.enroll_new()
        self.assertEqual((self.row(r1)['status'], self.row(r2)['status']), ('WAITING', 'WAITING'))
        for _ in range(8):
            self.tick(10)
            for r in (r1, r2):
                if self.row(r)['status'] == 'ACCEPTED':
                    self.observe(r, 'RETURNED')
        self.assertEqual(len(self.fake.starts_for(r1)), 1)
        self.assertEqual(len(self.fake.starts_for(r2)), 1)

    def test_two_assignments_with_the_same_text_are_two_work_items(self):
        first = self.assign(body='Run the migration.', title='Migrate staging')
        second = self.assign(body='Run the migration.', title='Migrate production')
        self.tick()
        self.assertEqual(self.row(first)['status'], 'ACCEPTED')
        self.assertEqual(self.row(second)['status'], 'BUSY')  # its own turn, after the first
        self.observe(first, 'RETURNED')
        self.tick(10)
        self.assertEqual(self.row(second)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(second)), 1)

    def test_a_plain_message_is_not_a_duplicate_of_work_with_the_same_text(self):
        self.deliver(body='Run the migration.', title='Migrate')
        note = self.assign(body='Run the migration.')
        self.engine.enroll_new()
        self.assertEqual(self.row(note)['status'], 'WAITING')

    def test_same_text_to_another_recipient_is_not_a_duplicate(self):
        self.deliver(body='Run the migration.')
        other = self.deliver(body='Run the migration.', recipient='reviewer')
        self.assertEqual(len(self.fake.starts_for(other)), 1)

    def test_idempotent_send_key_delivers_once(self):
        first = self.store.send('planner', 'engineer', 'Ship v1.', key='release-v1')
        again = self.store.send('planner', 'engineer', 'Ship v1.', key='release-v1')
        self.assertEqual(first, again)
        self.tick()
        self.tick(10)
        self.assertEqual(len(self.fake.starts), 1)


class BodyChangeTests(EngineTest):
    def test_changed_body_fails_and_is_never_sent(self):
        mid = self.assign()
        self.engine.enroll_new()
        self.tamper(mid, 'Delete the repository.')
        self.tick()
        row = self.row(mid)
        self.assertEqual(row['status'], 'FAILED')
        self.assertIn('changed after it was queued', row['detail'])
        for _ in range(5):
            self.tick(600)
        self.assertEqual(self.fake.starts, [])

    def test_body_change_after_delivery_does_not_misreport_the_delivered_turn(self):
        mid = self.deliver()
        self.tamper(mid, 'Something else.')
        self.observe(mid, 'RUNNING')
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'RUNNING')
        self.observe(mid, 'RETURNED')
        self.tick(10)
        self.assertEqual(self.row(mid)['status'], 'RETURNED')
        self.assertEqual(len(self.fake.starts), 1)


class HeldTests(EngineTest):
    def test_paused_delivery_is_held_until_resumed(self):
        self.store.configure(enabled=False)
        mid = self.assign()
        self.tick()
        self.assertEqual((self.row(mid)['status'], self.row(mid)['detail']), ('HELD', 'Delivery is paused.'))
        self.assertEqual(self.fake.starts, [])
        self.store.configure(enabled=True)
        self.tick(30)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_blocked_connection_is_held_until_allowed(self):
        mid = self.assign()
        self.store.set_connection('planner', 'engineer', allow=False)
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'HELD')
        self.assertEqual(self.fake.starts, [])
        self.store.set_connection('planner', 'engineer', allow=True)
        self.tick(30)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_explicit_mode_holds_pairs_that_were_never_allowed(self):
        mid = self.assign()
        self.store.configure(connections='explicit')
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'HELD')
        self.store.set_connection('planner', 'engineer', allow=True)
        self.tick(30)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_revoked_during_preparation_is_held_without_a_write(self):
        self.fake.on_prepare = lambda agent: self.store.set_connection('planner', 'engineer', allow=False)
        mid = self.assign()
        self.tick()
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('HELD', 0))
        self.assertIn('during preparation', row['detail'])
        self.assertEqual(self.fake.starts, [])


class EnrollmentTests(EngineTest):
    def test_messages_older_than_enabled_after_are_never_enrolled(self):
        old = self.assign(body='Backlog from before delivery was switched on.')
        self.clock.advance(5)
        self.store.configure(enabledAfter=self.clock.now)
        new = self.assign(body='Fresh task.')
        for _ in range(3):
            self.tick(60)
        self.assertIsNone(self.store.handoff(old))
        self.assertEqual(self.row(new)['status'], 'ACCEPTED')
        self.assertEqual(self.fake.starts_for(old), [])

    def test_notifications_and_already_read_messages_never_wake_anyone(self):
        note = self.store.send('planner', 'engineer', 'FYI: the build is green.', intent='notification')
        seen = self.assign(body='Already handled in person.')
        self.store.mark_read(seen, 'engineer')
        self.tick()
        self.assertIsNone(self.store.handoff(note))
        self.assertIsNone(self.store.handoff(seen))
        self.assertEqual(self.fake.starts, [])

    def test_message_read_after_enrollment_is_acknowledged_without_a_turn(self):
        mid = self.assign()
        self.engine.enroll_new()
        self.store.mark_read(mid, 'engineer')
        self.tick()
        self.assertEqual(self.row(mid)['status'], 'ACKNOWLEDGED')
        self.assertEqual(self.fake.starts, [])

    def test_mail_for_a_removed_agent_does_not_block_new_mail(self):
        self.store.register_agent('temp', 'Temp', provider='fake', endpoint='thread-temp')
        for n in range(3):
            self.assign(recipient='temp', body='Task ' + str(n))
            self.clock.advance(1)
        self.store.remove_agent('temp')
        fresh = self.assign(body='Fresh task.')
        self.assertEqual(self.engine.enroll_new(limit=3), [fresh])


class ReleaseTests(EngineTest):
    def test_an_operator_release_frees_a_stuck_recipient_without_a_resend(self):
        self.fake.start_script = [TimeoutError('no reply')]
        stuck = self.assign(body='First task.')
        self.tick()
        later = self.assign(body='Second task.')
        self.tick(10)
        self.assertEqual((self.row(stuck)['status'], self.row(later)['status']), ('UNCERTAIN', 'BUSY'))
        row = self.store.release(stuck, note='It never arrived; checked by hand.')
        self.assertEqual(row['status'], 'CANCELLED')
        self.assertEqual(row['receipt']['releasedFrom'], 'UNCERTAIN')
        self.assertIn('checked by hand', row['detail'])
        self.tick(10)
        self.assertEqual(self.row(later)['status'], 'ACCEPTED')
        for _ in range(10):
            self.tick(600)
        self.assertEqual(len(self.fake.starts_for(stuck)), 1)
        self.assertEqual(self.row(stuck)['status'], 'CANCELLED')

    def test_a_finished_delivery_cannot_be_released(self):
        mid = self.deliver()
        self.observe(mid, 'RETURNED')
        self.tick(10)
        with self.assertRaises(ValueError):
            self.store.release(mid)
        with self.assertRaises(ValueError):
            self.store.release('no-such-handoff')


class UnobservableTurnTests(EngineTest):
    def test_accepted_turn_nobody_can_observe_turns_uncertain_then_is_released_as_absent(self):
        mid = self.deliver()
        later = self.assign(body='Second task.')
        self.fake.observations['*'] = {'status': None, 'detail': 'No record of that run (the engine restarted).'}
        self.tick(POLL_FAST)
        for _ in range(UNOBSERVED_LIMIT // 60 - 1):
            self.tick(60)
            self.assertEqual(self.row(mid)['status'], 'ACCEPTED')
        self.tick(60)
        row = self.row(mid)
        self.assertEqual(row['status'], 'UNCERTAIN')
        self.assertIn('could not be observed for 10 min', row['detail'])
        self.assertIn('handoffs release ' + mid, row['detail'])
        self.assertEqual(self.row(later)['status'], 'BUSY')
        # From here the proven-absent rule applies to it like any UNCERTAIN row.
        self.fake.observations['*'] = {'status': None, 'receipt': ABSENT}
        for _ in range(20):
            if self.row(mid)['status'] != 'UNCERTAIN':
                break
            self.tick(300)
        self.assertEqual(self.row(mid)['status'], 'CANCELLED')
        self.tick(10)
        self.assertEqual(self.row(later)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(mid)), 1)

    def test_a_successful_observation_resets_the_bound(self):
        mid = self.deliver()
        self.fake.observations['*'] = {'status': None, 'detail': 'Temporarily unreadable.'}
        self.tick(POLL_FAST)
        self.tick(UNOBSERVED_LIMIT - 60)
        self.observe(mid, 'RUNNING')
        self.tick(60)
        self.assertEqual(self.row(mid)['status'], 'RUNNING')
        self.assertNotIn('unobservedSince', self.row(mid)['receipt'])
        del self.fake.observations[self.row(mid)['request_id']]
        self.tick(POLL_SLOW)
        self.tick(UNOBSERVED_LIMIT - 60)
        self.assertEqual(self.row(mid)['status'], 'RUNNING')
        self.tick(60)
        self.assertEqual(self.row(mid)['status'], 'UNCERTAIN')


class OneShotTests(EngineTest):
    def test_a_one_shot_engine_never_starts_a_turn_that_needs_a_host(self):
        self.fake.needs_host = True
        engine = self.make_engine(one_shot=True)
        mid = self.assign()
        self.tick(engine=engine)
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('WAITING', 0))
        self.assertIn('long-running engine', row['detail'])
        self.assertEqual((self.fake.prepares, self.fake.starts), ([], []))
        self.tick(60)  # a long-running engine delivers it
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_a_one_shot_engine_still_delivers_to_adapters_without_a_host(self):
        engine = self.make_engine(one_shot=True)
        mid = self.assign()
        self.tick(engine=engine)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')


class FairnessTests(EngineTest):
    def test_rows_waiting_behind_a_busy_recipient_do_not_starve_other_recipients(self):
        self.fake.start_script = [TimeoutError('reply lost')]  # engineer's first delivery goes UNCERTAIN
        self.assign(body='Job 0')
        self.tick()
        for n in range(1, SCAN_ROWS + 1):
            self.assign(body='Job ' + str(n))
            self.clock.advance(1)
        for _ in range(6):
            self.tick(5)  # the queue settles into BUSY
        urgent = self.assign(recipient='reviewer', body='Urgent job for the reviewer.')
        for _ in range(3):
            self.tick(5)
        self.assertEqual(self.row(urgent)['status'], 'ACCEPTED')
        self.assertEqual(len(self.fake.starts_for(urgent)), 1)

    def test_an_unchanged_busy_row_is_rechecked_later_not_every_pass(self):
        first = self.deliver(body='First task.')
        second = self.assign(body='Second task.')
        self.tick(POLL_FAST)
        self.assertEqual(self.row(second)['status'], 'BUSY')
        busy_events = len(self.store.events(second))
        self.tick(10)
        row = self.row(second)
        self.assertGreater(row['next_check'], self.clock.now)
        self.assertEqual(len(self.store.events(second)), busy_events)  # nothing new to log
        self.assertEqual(self.row(first)['status'], 'ACCEPTED')


    def test_a_queued_row_goes_on_the_next_pass_once_its_recipient_is_free(self):
        first = self.deliver(body='First task.')
        second = self.assign(body='Second task.')
        self.observe(first, 'RUNNING')
        self.tick(POLL_FAST)
        self.assertEqual(self.row(second)['status'], 'BUSY')  # its own next check is 10 s away
        self.observe(first, 'RETURNED')
        self.tick(POLL_FAST)
        self.assertEqual(self.row(first)['status'], 'RETURNED')
        self.tick()  # no waiting out the ten seconds
        self.assertEqual(self.row(second)['status'], 'ACCEPTED')


class PollingTests(EngineTest):
    def test_a_fresh_turn_is_checked_every_two_seconds_then_every_ten(self):
        mid = self.deliver()
        row = self.row(mid)
        self.assertEqual(row['next_check'] - row['updated'], POLL_FAST)
        self.observe(mid, 'RUNNING')
        self.tick(POLL_FAST)
        row = self.row(mid)
        self.assertEqual((row['status'], row['next_check'] - row['updated']), ('RUNNING', POLL_FAST))
        self.tick(POLL_FAST_WINDOW)
        row = self.row(mid)
        self.assertEqual(row['next_check'] - row['updated'], POLL_SLOW)

    def test_a_short_turn_is_seen_working_before_it_finishes(self):
        mid = self.deliver()
        self.observe(mid, 'RUNNING')
        self.tick(POLL_FAST)  # well before the old ten-second first check
        self.observe(mid, 'RETURNED')
        self.tick(POLL_FAST)
        statuses = [e['status'] for e in self.store.events(mid)]
        self.assertEqual(statuses[-3:], ['ACCEPTED', 'RUNNING', 'RETURNED'])


class TickTests(EngineTest):
    def test_adapter_bug_after_the_write_keeps_the_delivery_in_flight(self):
        self.fake.start_script = ['accepted']  # a buggy adapter: not a receipt dict
        mid = self.assign()
        later = self.assign(body='Next task.')
        report = self.tick()
        self.assertFalse(report['ok'])
        self.assertEqual([e['id'] for e in report['errors']], [mid])
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('UNCERTAIN', 1))
        for _ in range(5):
            self.tick(60)
        self.assertEqual(len(self.fake.starts_for(mid)), 1)
        self.assertEqual(self.row(later)['status'], 'BUSY')  # the recipient's slot is still taken
        self.assertEqual(self.fake.starts_for(later), [])

    def test_adapter_bug_before_the_write_spends_nothing(self):
        self.fake.on_prepare = lambda agent: 1 / 0
        mid = self.assign()
        report = self.tick()
        self.assertFalse(report['ok'])
        row = self.row(mid)
        self.assertEqual((row['status'], row['attempts']), ('UNAVAILABLE', 0))
        self.fake.on_prepare = None
        self.tick(30)
        self.assertEqual(self.row(mid)['status'], 'ACCEPTED')

    def test_tick_writes_a_health_report(self):
        self.assign()
        report = self.tick()
        health = json.loads(self.engine.health_path.read_text())
        self.assertEqual(health, json.loads(json.dumps(report)))
        self.assertEqual((health['enrolled'], health['checked'], health['ok']), (1, 1, True))
        self.assertEqual(health['pid'], os.getpid())
        self.assertEqual([p.name for p in self.dir.glob('*.tmp')], [])

    def test_run_forever_ticks_until_stopped(self):
        checks = []
        self.assign()
        self.engine.run_forever(interval=0, stop=lambda: checks.append(1) or len(checks) > 2)
        self.assertEqual(len(checks), 3)
        self.assertEqual(len(self.fake.starts), 1)


if __name__ == '__main__':
    unittest.main()
