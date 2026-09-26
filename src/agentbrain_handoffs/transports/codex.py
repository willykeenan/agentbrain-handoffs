"""Codex adapter (experimental): one handoff becomes one turn in an existing Codex thread.

It talks to ``codex app-server``, the JSON-RPC 2.0 interface the Codex apps use, over
stdio with one JSON message per line. The app server is used instead of ``codex exec``
because it can resume the recipient's *existing* thread by id and name the exact turn it
created, which is what the engine's exactly-once rules are built on.

Experimental: the app-server protocol is young and may change between Codex releases.
This adapter relies on:

- the handshake: an ``initialize`` request, then the ``initialized`` notification;
- ``thread/read`` for a thread's live status, ``thread/resume`` to load it,
  ``thread/turns/list`` for its history and ``turn/start`` to add one turn.

Worth knowing before you use it:

- The turn runs *inside* the app-server process this adapter started, so that process
  is kept alive until the turn is seen to finish, then closed so the thread is free for
  its owner again. Stopping the engine while a turn runs interrupts that turn; the
  engine's resend rules decide what happens next. For the same reason a one-shot
  ``handoffs tick`` never starts a Codex turn (``needs_host``).
- The app server runs with HANDOFFS_AGENT set to the recipient (and HANDOFFS_DB to the
  engine's database when known), so ``handoffs return`` inside the turn acts as the
  recipient, never as whoever started the engine.
- Nobody is at a keyboard to answer approval prompts. The adapter declines them with an
  error so a turn never hangs waiting; give delivered threads an approval policy that
  does not prompt.
- There is one app-server process per thread (the agent's endpoint), shared by reads
  and the running turn. Idle processes are closed after ``idle_seconds``.
"""
from __future__ import annotations

import collections
import contextlib
import itertools
import json
import os
import subprocess
import threading
import time
from pathlib import Path

from .. import __version__, cards
from ..transport import NotAccepted, Transport

PAGE_SIZE = 10
MAX_PAGES = 20
OWNER_IDLE_SECONDS = 6 * 3600  # a turn nobody has asked about for this long has lost its engine

CLIENT_INFO = {'name': 'agentbrain-handoffs', 'title': 'AgentBrain Handoffs', 'version': __version__}

# An error that says the call was refused before it ran. Only these may become NotAccepted,
# because only they prove no turn was created.
REJECTION_HINTS = ('active writer', 'already')
PRE_EXECUTION_CODES = (-32600, -32601, -32602)  # invalid request, unknown method, invalid params

# Latest turn status -> what the recipient is doing now (for activity()).
ACTIVITY_BY_TURN = {'inProgress': 'open', 'completed': 'finished', 'failed': 'failed', 'interrupted': 'stopped'}

# Our turn's status -> the delivery state the engine records (for observe()).
OBSERVED_BY_TURN = {
    'inProgress': ('RUNNING', 'The turn is running in Codex.'),
    'completed': ('RETURNED', 'Codex finished the turn.'),
    'failed': ('FAILED', 'The turn failed in Codex.'),
    'interrupted': ('FAILED', 'The turn was interrupted in Codex before it finished.'),
}

UNKNOWN = {'turnStatus': 'unknown', 'turnId': None}


class RpcError(RuntimeError):
    """The app server answered a request with a JSON-RPC error object."""

    def __init__(self, method, error):
        error = error if isinstance(error, dict) else {}
        self.code = error.get('code')
        self.message = str(error.get('message') or 'no reason given')
        super().__init__('Codex refused ' + method + ': ' + self.message)

    @property
    def rejected_before_running(self) -> bool:
        text = self.message.lower()
        return self.code in PRE_EXECUTION_CODES or any(hint in text for hint in REJECTION_HINTS)


class AppServer:
    """One ``codex app-server`` child process and its JSON-RPC conversation.

    A reader thread hands each response to the request waiting for it, so reads can
    share the process with a running turn. Requests the server sends us (approval
    prompts) are answered with an error, because a headless delivery cannot approve.
    """

    def __init__(self, argv, cwd=None, timeout=30.0, env=None):
        self.timeout = timeout
        self.stderr_tail = collections.deque(maxlen=20)
        self._ids = itertools.count(1)
        self._waiting = {}
        self._lock = threading.Lock()  # guards _waiting, _closed and every write to stdin
        self._closed = False
        try:
            self.proc = subprocess.Popen(list(argv) + ['app-server'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE, cwd=cwd or None, text=True, encoding='utf-8',
                                         errors='replace', bufsize=1, env=env)
        except OSError as e:
            raise RuntimeError('Could not start the Codex app server (' + str(argv[0]) + '): ' + (e.strerror or str(e)) +
                               '. Set the agent setting codex_bin or the CODEX_BIN environment variable.') from None
        threading.Thread(target=self._read_replies, name='codex-app-server-stdout', daemon=True).start()
        threading.Thread(target=self._read_stderr, name='codex-app-server-stderr', daemon=True).start()
        try:
            self.request('initialize', {'clientInfo': CLIENT_INFO})
            self.notify('initialized')
        except Exception:
            self.close()
            raise

    # ---- talking ------------------------------------------------------------------
    def request(self, method, params=None, timeout=None):
        """Send one request and wait for its answer. Raises RpcError, TimeoutError or ConnectionError."""
        slot = {'done': threading.Event()}
        with self._lock:
            if self._closed:
                raise ConnectionError(self._gone())
            request_id = next(self._ids)
            self._waiting[request_id] = slot
            try:
                self._write({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params or {}})
            except (OSError, ValueError):  # the pipe is gone: the process exited
                self._waiting.pop(request_id, None)
                raise ConnectionError(self._gone()) from None
        wait = self.timeout if timeout is None else timeout
        if not slot['done'].wait(wait):
            with self._lock:
                self._waiting.pop(request_id, None)
            raise TimeoutError('The Codex app server did not answer ' + method + ' within ' + format(wait, 'g') + ' s.')
        reply = slot.get('reply')
        if reply is None:
            raise ConnectionError(self._gone())
        if 'error' in reply:
            raise RpcError(method, reply['error'])
        result = reply.get('result')
        return result if isinstance(result, dict) else {}

    def notify(self, method, params=None):
        message = {'jsonrpc': '2.0', 'method': method}
        if params is not None:
            message['params'] = params
        with self._lock:
            try:
                if self._closed:
                    raise ValueError('closed')
                self._write(message)
            except (OSError, ValueError):
                raise ConnectionError(self._gone()) from None

    def _write(self, message):
        self.proc.stdin.write(json.dumps(message) + '\n')
        self.proc.stdin.flush()

    # ---- listening ----------------------------------------------------------------
    def _read_replies(self):
        for line in self.proc.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue  # not protocol output (a stray log line); the protocol is JSON only
            if not isinstance(message, dict):
                continue
            if 'method' in message:
                if 'id' in message:
                    self._decline(message)
                continue  # notifications report progress we do not need; history is read on demand
            with self._lock:
                slot = self._waiting.pop(message.get('id'), None)
            if slot is not None:
                slot['reply'] = message
                slot['done'].set()
        with self._lock:
            self._closed = True
            waiting, self._waiting = self._waiting, {}
        for slot in waiting.values():
            slot['done'].set()  # no reply: the caller raises ConnectionError

    def _decline(self, message):
        """Answer a server request (an approval prompt) so the turn never waits on us forever."""
        error = {'code': -32601, 'message': 'AgentBrain Handoffs delivers turns headlessly and cannot answer '
                 + str(message.get('method')) + '. Use an approval policy that does not prompt.'}
        with self._lock, contextlib.suppress(OSError, ValueError):
            if not self._closed:
                self._write({'jsonrpc': '2.0', 'id': message['id'], 'error': error})

    def _read_stderr(self):
        for line in self.proc.stderr:
            if line.strip():
                self.stderr_tail.append(line.rstrip())

    # ---- lifetime -----------------------------------------------------------------
    def alive(self) -> bool:
        return not self._closed and self.proc.poll() is None

    def _gone(self) -> str:
        last = self.stderr_tail[-1] if self.stderr_tail else ''
        return 'The Codex app server exited' + (': ' + last[:300] if last else '.')

    def close(self):
        """End the conversation. A turn still running inside this process is interrupted."""
        with self._lock:
            self._closed = True
            with contextlib.suppress(OSError, ValueError):
                self.proc.stdin.close()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


class _Session:
    """A pooled app server for one thread, and what this adapter is doing with it."""

    def __init__(self, server):
        self.server = server
        self.owner = None     # request id of the turn started in this process; it dies with the process
        self.loaded = False   # this process resumed (loaded) the thread
        self.holds = 0        # prepare() blocks currently using it
        self.used = time.monotonic()


class CodexTransport(Transport):
    """Delivers into Codex threads through ``codex app-server``. Experimental.

    The agent's endpoint is the thread id. The Codex command is, most specific first:
    the agent setting ``codex_bin``, the ``binary`` given here, ``$CODEX_BIN``, then
    ``codex``. Each may be a path or an argv list (for example a wrapper script).
    """

    name = 'codex'
    needs_host = True  # the turn runs inside the app-server process this adapter started

    def __init__(self, binary=None, timeout=30.0, idle_seconds=60.0, store=None):
        self.binary = binary
        self._db = str(Path(store.path).absolute()) if store is not None else None
        self.timeout = timeout
        self.idle_seconds = idle_seconds
        self._sessions = {}
        self._lock = threading.Lock()

    def command(self, agent) -> list:
        """The argv prefix that runs Codex for this agent."""
        settings = agent.get('settings') or {}
        for value in (settings.get('codex_bin'), self.binary, os.environ.get('CODEX_BIN'), 'codex'):
            if value:
                return _argv(value, 'codex_bin')
        return ['codex']

    # ---- the adapter contract -----------------------------------------------------
    def activity(self, agent):
        """What the thread is doing now. Any error means 'unknown', so nothing is sent blind."""
        endpoint = agent.get('endpoint') or ''
        if not endpoint:
            return dict(UNKNOWN)
        try:
            key, session = self._session(agent)
            state = _kind(self._read_thread(session, endpoint).get('status'))
            if state == 'active':
                return {'turnStatus': 'open', 'turnId': self._running_turn_id(session, endpoint)}
            if state not in ('idle', 'notLoaded'):
                return dict(UNKNOWN)
            latest = self._latest_turn(session, endpoint)
        except Exception:
            return dict(UNKNOWN)
        if latest is None:
            return {'turnStatus': 'finished', 'turnId': None}
        status = ACTIVITY_BY_TURN.get(_kind(latest.get('status')), 'unknown')
        if session.owner and status != 'open':
            self._release(key, session)  # the turn this process was running is over; free the thread
        return {'turnStatus': status, 'turnId': latest.get('id')}

    @contextlib.contextmanager
    def prepare(self, agent):
        """Check the exact thread and load it into our app server. Adds no turn."""
        endpoint = agent.get('endpoint') or ''
        if not endpoint:
            raise ValueError('This Codex agent has no thread id. Set its endpoint to the thread id.')
        key, session = self._session(agent)
        with self._lock:
            session.holds += 1
        try:
            try:
                thread = self._read_thread(session, endpoint)
                if thread.get('id') != endpoint:
                    raise RuntimeError('Codex answered with thread ' + repr(thread.get('id')) + ' when asked for ' +
                                       repr(endpoint) + '; nothing is sent to a different thread.')
                if _kind(thread.get('status')) == 'notLoaded':
                    session.server.request('thread/resume', {'threadId': endpoint})
                    session.loaded = True
            except RpcError as e:
                if any(hint in e.message.lower() for hint in REJECTION_HINTS):
                    raise NotAccepted(str(e)) from None
                raise
            yield {'agent': agent, 'endpoint': endpoint, 'key': key, 'session': session}
        finally:
            with self._lock:
                session.holds -= 1
            if session.owner is None and session.loaded:
                self._release(key, session)  # no turn was started: let the thread's owner have it back

    def start(self, prepared, text, request_id):
        """The one provider write: ``turn/start`` with our request id as the client message id."""
        session, endpoint = prepared['session'], prepared['endpoint']
        previous = session.owner
        session.owner = request_id  # set first: if the reply is lost, the turn may be running in this process
        try:
            result = session.server.request('turn/start', {
                'threadId': endpoint,
                'input': [{'type': 'text', 'text': text}],
                'clientUserMessageId': request_id,
            })
        except RpcError as e:
            if not e.rejected_before_running:
                raise  # the turn may exist; the engine records UNCERTAIN and reconciles
            session.owner = previous
            raise NotAccepted(str(e)) from None
        receipt = {'clientUserMessageId': request_id, 'threadId': endpoint}
        turn = result.get('turn')
        if isinstance(turn, dict) and turn.get('id'):
            receipt['turnId'] = turn['id']
        return receipt

    def observe(self, agent, request_id, receipt):
        """Find our turn in the thread history, by turn id or by the delivery marker."""
        endpoint = agent.get('endpoint') or ''
        receipt = receipt or {}
        try:
            key, session = self._session(agent)
            turn, candidate, complete = self._search(session, endpoint, request_id, receipt.get('turnId'))
        except Exception as e:
            return {'status': None, 'detail': 'Could not read the Codex thread history: ' + str(e)[:300]}
        if turn is None:
            return self._not_found(candidate, complete)
        kind = _kind(turn.get('status'))
        status, detail = OBSERVED_BY_TURN.get(kind, (None, 'Codex reports the turn as ' + (kind or 'unknown') + '.'))
        found = {'observedTurnId': turn.get('id'), 'turnStatus': kind}
        if not receipt.get('turnId') and turn.get('id'):
            found['turnId'] = turn['id']
        if kind == 'interrupted':
            found['turnOutcome'] = 'interrupted'
        if kind == 'failed':
            reason = (turn.get('error') or {}).get('message') if isinstance(turn.get('error'), dict) else None
            if reason:
                detail = 'The turn failed in Codex: ' + str(reason)[:300]
        if status in ('RETURNED', 'FAILED') and session.owner == request_id:
            self._release(key, session)  # our turn is over; free the thread
        return {'status': status, 'detail': detail, 'receipt': found}

    def close(self):
        """Close every app server this adapter started. Running turns inside them are interrupted."""
        with self._lock:
            sessions, self._sessions = list(self._sessions.values()), {}
        for session in sessions:
            session.server.close()

    # ---- reading a thread ---------------------------------------------------------
    @staticmethod
    def _read_thread(session, endpoint) -> dict:
        result = session.server.request('thread/read', {'threadId': endpoint, 'includeTurns': False})
        thread = result.get('thread')
        if not isinstance(thread, dict):
            raise RuntimeError('Codex returned no thread for ' + repr(endpoint) + '.')
        return thread

    @staticmethod
    def _latest_turn(session, endpoint):
        page = session.server.request('thread/turns/list', {'threadId': endpoint, 'limit': 1, 'sortDirection': 'desc'})
        turns = _turns(page)
        return turns[0] if turns else None

    def _running_turn_id(self, session, endpoint):
        """Best effort: the id of the in-progress turn, so the engine can tell turns apart."""
        try:
            latest = self._latest_turn(session, endpoint)
        except Exception:
            return None
        return latest.get('id') if latest and _kind(latest.get('status')) == 'inProgress' else None

    @staticmethod
    def _search(session, endpoint, request_id, turn_id):
        """Walk the history newest first. Returns (turn or None, candidate turn id, search complete).

        A candidate is a turn whose user text mentions the request id without being the
        delivery itself (quoted text). It is reported so the engine never treats a
        history that merely talks about a handoff as proof the handoff is absent.
        """
        cursor, seen, candidate = None, set(), None
        for _ in range(MAX_PAGES):
            params = {'threadId': endpoint, 'limit': PAGE_SIZE, 'sortDirection': 'desc'}
            if cursor:
                params['cursor'] = cursor
            page = session.server.request('thread/turns/list', params)
            for turn in _turns(page):
                texts = _user_texts(turn)
                if (turn_id and turn.get('id') == turn_id) or any(cards.marker_matches(t, request_id) for t in texts):
                    return turn, None, True
                if candidate is None and any(request_id in t for t in texts):
                    candidate = turn.get('id') or 'unnamed turn'
            cursor = page.get('nextCursor')
            if not cursor:
                return None, candidate, True
            if cursor in seen:
                return None, candidate, False  # the server repeated a page; stop instead of looping
            seen.add(cursor)
        return None, candidate, False

    @staticmethod
    def _not_found(candidate, complete):
        if complete and candidate is None:
            detail = 'The whole thread history was searched and this delivery is not in it.'
        elif complete:
            detail = 'No delivered turn found; turn ' + str(candidate) + ' only mentions this handoff.'
        else:
            detail = 'Not found in the newest ' + str(PAGE_SIZE * MAX_PAGES) + ' turns; the search is incomplete.'
        return {'status': None, 'detail': detail,
                'receipt': {'historySearch': {'exhausted': complete, 'candidate': candidate, 'turnId': None}}}

    # ---- the process pool ---------------------------------------------------------
    def _session(self, agent):
        """The live app server for this agent's thread, started if needed."""
        key = (tuple(self.command(agent)), agent.get('endpoint') or '')
        with self._lock:
            stale = self._reap()
            session = self._sessions.get(key)
            if session is not None:
                session.used = time.monotonic()
        for old in stale:
            old.server.close()
        if session is not None:
            return key, session
        cwd = agent.get('cwd') or None
        # Turns run inside this process, so its environment names the recipient: tools the
        # turn runs (`handoffs return`) must not inherit the engine's HANDOFFS_AGENT.
        env = dict(os.environ, HANDOFFS_AGENT=str(agent.get('id') or ''))
        if self._db:
            env['HANDOFFS_DB'] = self._db
        fresh = _Session(AppServer(list(key[0]), cwd=cwd if cwd and os.path.isdir(cwd) else None, timeout=self.timeout,
                                   env=env))
        with self._lock:
            session = self._sessions.setdefault(key, fresh)
        if session is not fresh:
            fresh.server.close()  # another caller started one at the same moment; keep theirs
        return key, session

    def _reap(self):
        """Drop dead processes and pick idle ones to close. Call with the lock held."""
        now = time.monotonic()
        stale = []
        for key, session in list(self._sessions.items()):
            idle = now - session.used
            if not session.server.alive():
                del self._sessions[key]
            elif session.holds:
                continue
            elif (session.owner is None and idle > self.idle_seconds) or idle > OWNER_IDLE_SECONDS:
                del self._sessions[key]
                stale.append(session)
        return stale

    def _release(self, key, session):
        with self._lock:
            if self._sessions.get(key) is session:
                del self._sessions[key]
        session.server.close()


# ---- small helpers ----------------------------------------------------------------
def _argv(value, setting) -> list:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and value and all(isinstance(v, str) and v for v in value):
        return list(value)
    raise ValueError(setting + ' must be a path or a list of strings')


def _kind(status) -> str:
    """Statuses arrive as 'idle' or as {'type': 'idle', ...}; return the name either way."""
    if isinstance(status, dict):
        status = status.get('type')
    return status if isinstance(status, str) else ''


def _turns(page) -> list:
    turns = page.get('data') if isinstance(page.get('data'), list) else page.get('turns')
    return [t for t in turns if isinstance(t, dict)] if isinstance(turns, list) else []


def _user_texts(turn) -> list:
    """The text of a turn's user messages: the only place a real delivery can appear."""
    texts = []
    for item in turn.get('items') or []:
        if not isinstance(item, dict) or item.get('type') != 'userMessage':
            continue
        content = item.get('content')
        if isinstance(content, str):
            texts.append(content)
        for part in content if isinstance(content, list) else []:
            if isinstance(part, dict) and part.get('type') == 'text' and isinstance(part.get('text'), str):
                texts.append(part['text'])
    return texts
