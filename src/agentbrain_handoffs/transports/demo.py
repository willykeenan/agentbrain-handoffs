"""Simulated agents, so you can watch handoffs move without any real agent app.

Each delivery becomes one simulated turn that lasts a few seconds and then finishes,
fails or is stopped. When a finished turn was a work assignment, the demo agent
returns the work through the store, exactly as a real agent would with
`handoffs return`. That closes the loop the engine is built for, so the demo
exercises the same states, resends and returns as production.

Turns live in a small JSON file (by default next to the database) so the engine,
the web page and the command line agree even when they run as separate processes.
Everything is deterministic for a given seed and request id, which keeps tests
and recorded demos repeatable.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import random
import tempfile
import threading
import time
import uuid
from pathlib import Path

from .. import cards
from ..transport import NotAccepted, Transport

MIN_SECONDS = 4.0
MAX_SECONDS = 15.0
MAX_TURNS = 1000  # older settled turns are dropped so the state file stays small

# Share of simulated turns by outcome. Everything else is 'stopped'.
FINISHED_SHARE = 0.80
FAILED_SHARE = 0.10

SUMMARIES = (
    'Implemented the change and added tests; the full suite passes.',
    'Reproduced the bug, fixed the root cause and added a regression test.',
    'Reviewed the diff and applied two small fixes; ready to merge.',
    'Drafted the section and linked the supporting notes.',
    'Updated the docs and checked that every example runs.',
    'Compared both approaches; the simpler one is fast enough, so it is in.',
    'Split the task into three steps; the first is done and verified.',
    'Cleaned up the configuration and confirmed the build still passes.',
)

DETAILS = {
    'open': 'The demo agent is working on it.',
    'finished': 'The demo agent finished the turn.',
    'failed': "The demo agent's turn failed (simulated error).",
    'stopped': "The demo agent's turn was stopped before it finished (simulated).",
}
OBSERVED = {'open': 'RUNNING', 'finished': 'RETURNED', 'failed': 'FAILED', 'stopped': 'FAILED'}
OUTCOMES = {'finished': 'completed', 'failed': 'failed', 'stopped': 'interrupted'}


class DemoTransport(Transport):
    """Adapter for provider 'demo': simulated agents with repeatable behavior.

    store: optional Store. With it, finished assignments are returned as real work
        results, and turns persist next to the database by default.
    state_path: where turns persist. Without a store or a path, turns live in
        memory for this process only.
    seed: changes which outcome and duration each request gets.
    speed: divides every turn's duration (2.0 runs twice as fast).
    clock: seconds since the epoch; tests pass a fake one.
    """

    name = 'demo'

    def __init__(self, store=None, state_path=None, seed=0, speed=1.0, clock=time.time):
        if not isinstance(speed, (int, float)) or isinstance(speed, bool) or speed <= 0:
            raise ValueError('speed must be a number above zero')
        self.store = store
        if state_path is None and store is not None:
            state_path = store.path.with_name(store.path.name + '.demo-turns.json')
        self.state_path = Path(state_path) if state_path is not None else None
        self.seed = seed
        self.speed = float(speed)
        self.clock = clock
        self._lock = threading.RLock()
        self._memory = {'turns': [], 'forgotten': False}

    # ---- the simulation ------------------------------------------------------------
    def plan(self, request_id: str):
        """What the simulation does with this request: (outcome, seconds, summary).

        Derived only from (seed, request id), so every process and every rerun
        makes the same choice. String seeds hash the same way in every process.
        """
        rng = random.Random('%s:%s' % (self.seed, request_id))
        roll = rng.random()
        if roll < FINISHED_SHARE:
            outcome = 'finished'
        elif roll < FINISHED_SHARE + FAILED_SHARE:
            outcome = 'failed'
        else:
            outcome = 'stopped'
        seconds = rng.uniform(MIN_SECONDS, MAX_SECONDS) / self.speed
        return outcome, seconds, rng.choice(SUMMARIES)

    def advance(self) -> int:
        """Settle every turn whose time is up. Returns how many settled.

        Reads already do this, so calling it is optional; it is handy for a feeder
        loop that wants returned work to appear without waiting for the engine.
        """
        with self._state() as state:
            return self._settle(state)

    # ---- the adapter contract ------------------------------------------------------
    def activity(self, agent: dict) -> dict:
        """The agent's latest turn. An agent that never had one is idle ('finished').

        Reading also settles turns whose time is up: a real agent would have
        finished them in the meantime, so the answer stays truthful.
        """
        with self._state() as state:
            self._settle(state)
            turn = self._latest(state, agent.get('id'))
        if turn is None:
            return {'turnStatus': 'finished', 'turnId': None}
        return {'turnStatus': turn['status'], 'turnId': turn['id']}

    def start(self, prepared: dict, text: str, request_id: str) -> dict:
        """Begin one simulated turn. Refuses, like a real app, while a turn is open."""
        agent = prepared['agent']
        with self._state() as state:
            self._settle(state)
            turn = self._find(state, agent.get('id'), request_id=request_id)  # never a second turn per request
            latest = self._latest(state, agent.get('id'))
            busy = turn is None and latest is not None and latest['status'] == 'open'
            if turn is None and not busy:
                outcome, seconds, summary = self.plan(request_id)
                now = self.clock()
                turn = {'id': str(uuid.uuid4()), 'requestId': request_id, 'agent': agent.get('id'),
                        'created': now, 'endsAt': now + seconds, 'outcome': outcome, 'status': 'open',
                        'summary': summary, 'messageId': assignment_id(text, request_id), 'workReturned': False}
                state['turns'].append(turn)
                self._prune(state)
        if busy:
            raise NotAccepted('The demo agent is already in a turn.')
        return {'turnId': turn['id'], 'clientUserMessageId': request_id}

    def observe(self, agent: dict, request_id: str, receipt: dict) -> dict:
        """What happened to this request's turn, found by receipt turn id or request id."""
        with self._state() as state:
            self._settle(state)
            turn = self._find(state, agent.get('id'), turn_id=(receipt or {}).get('turnId'), request_id=request_id)
            complete = self.state_path is not None and not state['forgotten']
        if turn is None:
            result = {'status': None, 'detail': 'The demo agent has no turn for this request.', 'receipt': {}}
            if complete:
                # Every turn this demo ever made is on file, so absence is proven.
                result['receipt'] = {'historySearch': {'exhausted': True, 'candidate': None, 'turnId': None}}
            return result
        found = {'turnId': turn['id'], 'turnStatus': turn['status']}
        if turn['status'] != 'open':
            found['turnOutcome'] = OUTCOMES[turn['status']]
        detail = DETAILS[turn['status']]
        if turn['workReturned']:
            detail += ' It returned the work: ' + turn['summary']
        return {'status': OBSERVED[turn['status']], 'detail': detail, 'receipt': found}

    # ---- turn bookkeeping ----------------------------------------------------------
    @staticmethod
    def _latest(state, agent_id):
        for turn in reversed(state['turns']):
            if turn['agent'] == agent_id:
                return turn
        return None

    @staticmethod
    def _find(state, agent_id, turn_id=None, request_id=None):
        """Only this agent's turns count, so one agent's turn never answers for another."""
        for turn in reversed(state['turns']):
            if turn['agent'] == agent_id and ((turn_id and turn['id'] == turn_id) or turn['requestId'] == request_id):
                return turn
        return None

    def _settle(self, state) -> int:
        now = self.clock()
        settled = 0
        for turn in state['turns']:
            if turn['status'] == 'open' and now >= turn['endsAt']:
                turn['status'] = turn['outcome']
                if turn['status'] == 'finished':
                    turn['workReturned'] = self._return_work(turn)
                settled += 1
        return settled

    def _return_work(self, turn) -> bool:
        """Return the assignment as the demo agent. Plain messages and replies have no work."""
        if self.store is None or not turn.get('messageId'):
            return False
        work = self.store.work(turn['messageId'])
        if not work or work['returned'] or work['closed']:
            return False
        try:
            self.store.return_work(turn['messageId'], turn['agent'], turn['summary'])
        except ValueError:
            return False  # not this agent's work, or someone returned it first
        return True

    @staticmethod
    def _prune(state):
        extra = len(state['turns']) - MAX_TURNS
        if extra <= 0:
            return
        keep = []
        for turn in state['turns']:
            if extra > 0 and turn['status'] != 'open':
                extra -= 1
                state['forgotten'] = True  # from now on a missing turn is not proof of absence
                continue
            keep.append(turn)
        state['turns'] = keep

    # ---- storage -------------------------------------------------------------------
    @contextlib.contextmanager
    def _state(self):
        """Hold the turn table for one read-modify-write.

        A thread lock covers this process; a file lock covers other processes that
        share the state file. Writes are atomic (temp file, then rename), so a
        reader never sees half a file. Never nest this: the file lock is not reentrant.
        """
        with self._lock:
            if self.state_path is None:
                yield self._memory
                return
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path = self.state_path.with_name(self.state_path.name + '.lock')
            with open(lock_path, 'a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX)
                state = self._load()
                before = json.dumps(state, sort_keys=True)
                yield state
                after = json.dumps(state, sort_keys=True)
                if after != before:
                    self._save(after)

    def _load(self) -> dict:
        try:
            state = json.loads(self.state_path.read_text())
            if isinstance(state, dict) and isinstance(state.get('turns'), list):
                state.setdefault('forgotten', False)
                return state
        except FileNotFoundError:
            return {'turns': [], 'forgotten': False}
        except (OSError, ValueError):
            pass
        # A damaged file loses its history, so absence can no longer be proven.
        return {'turns': [], 'forgotten': True}

    def _save(self, text):
        fd, tmp = tempfile.mkstemp(prefix=self.state_path.name + '.', suffix='.tmp', dir=str(self.state_path.parent))
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(text + '\n')
            os.replace(tmp, self.state_path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise


def assignment_id(text, request_id):
    """The inbox message id from a real delivery's 'Message: <id>' line, else None.

    Only a delivery that carries this request's marker is trusted, and only the
    technical block after the marker is read, so quoted card text cannot redirect it.
    """
    if not cards.marker_matches(text, request_id):
        return None
    technical = text if text.startswith('HANDOFF ') else text.partition(cards.DETAILS)[2]
    for line in technical.splitlines()[1:4]:
        if line.startswith('Message: '):
            return line[len('Message: '):].strip() or None
    return None
