"""Claude Code adapter (experimental): one handoff becomes one turn in an existing session.

Each delivery runs Claude Code's headless mode against the recipient's session:

    claude -p --resume <session id> --output-format stream-json --verbose

in the agent's working directory, with the handoff text on stdin, and follows the JSON
event stream it prints. The agent's endpoint is the session id.

Experimental: this relies on Claude Code's headless flags, its stream-json events
(``system``/``init`` first, ``result`` last) and its session transcripts at
``~/.claude/projects/*/<session id>.jsonl`` (``$CLAUDE_CONFIG_DIR/projects`` when set).

Worth knowing before you use it:

- Claude Code has no live "is this session busy?" API, so activity is inferred. Our own
  delivery process running means busy. Otherwise a transcript written in the last 90
  seconds means someone is using the session interactively, and so does a transcript
  whose last turn has not ended: a tool call still running, or a prompt or tool result
  the model has not answered yet. That lasts up to ``open_cap`` seconds (default one
  hour) of silence; a turn quiet for longer counts as stopped. Only a turn that ended
  (``end_turn``, an API error or an interruption) counts as idle sooner.
- Claude Code finds sessions per project, so set the agent's cwd to the directory the
  session was started in.
- Headless mode cannot answer permission prompts; give the session the permissions its
  work needs.
- The delivery process is a child of the engine, so stopping the engine can stop the
  turn. After a restart the transcript is searched to learn what happened to it. For
  the same reason a one-shot ``handoffs tick`` never starts a Claude Code turn
  (``needs_host``); run ``handoffs run`` or ``handoffs serve``.
- The delivery runs with HANDOFFS_AGENT set to the recipient (and HANDOFFS_DB to the
  engine's database when known), so ``handoffs return`` inside the turn acts as the
  recipient, never as whoever started the engine.
"""
from __future__ import annotations

import collections
import contextlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

from .. import cards
from ..transport import NotAccepted, Transport

RECENT_SECONDS = 90       # a transcript written this recently means the session is in use
OPEN_CAP_SECONDS = 3600   # an unfinished turn silent this long is treated as stopped
TAIL_BYTES = 256 * 1024   # how much of the transcript's end activity() reads
LOCAL_COMMAND_TAGS = ('<command-name>', '<command-message>', '<command-args>', '<local-command-stdout>',
                      '<local-command-stderr>', '<local-command-caveat>')
INTERRUPTED_PREFIX = '[Request interrupted'
START_TIMEOUT = 60.0      # how long start() waits for the session to confirm it resumed
KEEP_FINISHED = 256       # finished runs remembered for activity() and observe()
HEADLESS_FLAGS = ('--output-format', 'stream-json', '--verbose')
FINISHED_STOPS = ('end_turn', 'stop_sequence')
SESSION_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}')  # also keeps glob characters out of paths

UNKNOWN = {'turnStatus': 'unknown', 'turnId': None}


class _Run:
    """One headless ``claude -p`` process working on one delivered turn."""

    def __init__(self, request_id, session_id, proc):
        self.request_id = request_id
        self.session_id = session_id
        self.proc = proc
        self.lines = 0                 # stdout lines seen, JSON or not
        self.init = None               # the system/init event: the session resumed
        self.result = None             # the final result event
        self.exit_code = None
        self.ended_at = None           # wall-clock time the process exited (compared with file times)
        self.stderr_tail = collections.deque(maxlen=20)
        self.ready = threading.Event()  # init or result seen, or the output ended
        self.done = threading.Event()   # the process exited and its output is drained
        self._stderr_reader = None

    def follow(self, text):
        """Feed the prompt and read both output streams in the background."""
        threading.Thread(target=self._feed, args=(text.encode('utf-8'),), name='claude-stdin', daemon=True).start()
        self._stderr_reader = threading.Thread(target=self._read_stderr, name='claude-stderr', daemon=True)
        self._stderr_reader.start()
        threading.Thread(target=self._read_events, name='claude-stdout', daemon=True).start()

    def _feed(self, data):
        try:
            self.proc.stdin.write(data)
        except OSError:
            pass  # it exited before reading everything; the exit code tells the story
        finally:
            with contextlib.suppress(OSError):
                self.proc.stdin.close()

    def _read_events(self):
        for raw in self.proc.stdout:
            line = raw.decode('utf-8', 'replace').strip()
            if not line:
                continue
            self.lines += 1
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            if event.get('type') == 'system' and event.get('subtype') == 'init' and self.init is None:
                self.init = event
                self.ready.set()
            elif event.get('type') == 'result':
                self.result = event
                self.ready.set()
        self.exit_code = self.proc.wait()
        self.ended_at = time.time()
        self.done.set()
        self.ready.set()

    def _read_stderr(self):
        for raw in self.proc.stderr:
            line = raw.decode('utf-8', 'replace').strip()
            if line:
                self.stderr_tail.append(line)

    def reason(self) -> str:
        """The last line Claude Code printed on stderr, as a sentence ending."""
        if self._stderr_reader is not None:
            self._stderr_reader.join(timeout=1)
        return ': ' + self.stderr_tail[-1][:300] if self.stderr_tail else '.'

    def session_ids(self) -> list:
        """The session we resumed, plus the one Claude Code reported if it differs."""
        reported = (self.init or self.result or {}).get('session_id')
        return [self.session_id] + ([reported] if reported and reported != self.session_id else [])

    def outcome(self) -> str:
        """How the turn ended, in activity() terms."""
        if self.result is not None:
            return 'failed' if self.result.get('is_error') else 'finished'
        if self.exit_code is not None and self.exit_code < 0:
            return 'stopped'
        return 'finished' if self.exit_code == 0 else 'failed'


class ClaudeCodeTransport(Transport):
    """Delivers into Claude Code sessions through headless mode. Experimental.

    The Claude Code command is, most specific first: the agent setting ``claude_bin``,
    the ``binary`` given here, ``$CLAUDE_BIN``, then ``claude``. Each may be a path or
    an argv list (for example a wrapper script).
    """

    name = 'claude-code'
    needs_host = True  # the turn lives in a child process of the engine that starts it

    def __init__(self, binary=None, start_timeout=START_TIMEOUT, store=None, open_cap=OPEN_CAP_SECONDS):
        self.binary = binary
        self.start_timeout = start_timeout
        self.open_cap = open_cap
        self._db = str(Path(store.path).absolute()) if store is not None else None
        self._runs = collections.OrderedDict()  # request id -> _Run, oldest first
        self._lock = threading.Lock()

    def command(self, agent) -> list:
        """The argv prefix that runs Claude Code for this agent."""
        settings = agent.get('settings') or {}
        for value in (settings.get('claude_bin'), self.binary, os.environ.get('CLAUDE_BIN'), 'claude'):
            if value:
                return _argv(value, 'claude_bin')
        return ['claude']

    @staticmethod
    def projects_dir() -> Path:
        base = os.environ.get('CLAUDE_CONFIG_DIR')
        return (Path(base).expanduser() if base else Path.home() / '.claude') / 'projects'

    def transcripts(self, session_id) -> list:
        """Transcript files for a session id (normally one)."""
        if not isinstance(session_id, str) or not SESSION_ID.fullmatch(session_id):
            return []
        try:
            return sorted(self.projects_dir().glob('*/' + session_id + '.jsonl'))
        except OSError:
            return []

    # ---- the adapter contract -----------------------------------------------------
    def activity(self, agent):
        """Busy while our delivery runs; otherwise judged by how recently the transcript changed."""
        session_id = agent.get('endpoint') or ''
        if not SESSION_ID.fullmatch(session_id):
            return dict(UNKNOWN)
        run = self._latest_run(session_id)
        if run is not None and not run.done.is_set():
            return {'turnStatus': 'open', 'turnId': run.request_id}
        modified = self._last_modified(run.session_ids() if run else [session_id])
        if run is not None and (modified is None or modified <= run.ended_at + 1):
            # Nothing has touched the session since our own turn ended, so that turn is the latest.
            return {'turnStatus': run.outcome(), 'turnId': run.request_id}
        if modified is None:
            return dict(UNKNOWN)
        age = time.time() - modified
        if age < RECENT_SECONDS:
            return {'turnStatus': 'open', 'turnId': None}
        ending = self._last_ending(run.session_ids() if run else [session_id])
        if ending == 'open':
            # A tool call (a long test run, say) or an unanswered prompt: the owner's turn is
            # still going even though nothing was written for a while.
            return {'turnStatus': 'open' if age < self.open_cap else 'stopped', 'turnId': None}
        return {'turnStatus': ending or 'finished', 'turnId': None}

    @contextlib.contextmanager
    def prepare(self, agent):
        """Check the exact session, its directory and the command exist. Runs nothing."""
        session_id = agent.get('endpoint') or ''
        if not SESSION_ID.fullmatch(session_id):
            raise ValueError('The endpoint must be a Claude Code session id; got ' + repr(session_id[:80]) + '.')
        if not self.transcripts(session_id):
            raise ValueError('No Claude Code session ' + session_id + ' was found in ' + str(self.projects_dir()) + '.')
        cwd = agent.get('cwd') or None
        if cwd and not os.path.isdir(cwd):
            raise ValueError('The working directory ' + cwd + ' does not exist.')
        argv = self.command(agent)
        if not _runnable(argv[0]):
            raise ValueError('Claude Code was not found (' + argv[0] + '). Set the agent setting claude_bin or '
                             'the CLAUDE_BIN environment variable.')
        yield {'agent': agent, 'session_id': session_id, 'cwd': cwd, 'argv': argv}

    def start(self, prepared, text, request_id):
        """The one provider write: a headless run that resumes the session with our text."""
        session_id = prepared['session_id']
        argv = prepared['argv'] + ['-p', '--resume', session_id, *HEADLESS_FLAGS]
        # The turn acts as the recipient: `handoffs return --as` defaults to HANDOFFS_AGENT,
        # which must never be inherited from whoever started the engine.
        env = dict(os.environ, HANDOFFS_AGENT=str(prepared['agent'].get('id') or ''))
        if self._db:
            env['HANDOFFS_DB'] = self._db
        try:
            proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    cwd=prepared['cwd'], env=env)
        except OSError as e:
            raise NotAccepted('Could not run Claude Code: ' + (e.strerror or str(e))) from None
        run = _Run(request_id, session_id, proc)
        with self._lock:
            self._runs[request_id] = run
            self._forget_old_runs()
        run.follow(text)
        if not run.ready.wait(self.start_timeout):
            raise RuntimeError('Claude Code did not confirm the session within ' + format(self.start_timeout, 'g') +
                               ' s; the delivery is followed by observation.')
        confirmed = run.init or run.result
        if confirmed is not None:
            return {'turnId': request_id, 'clientUserMessageId': request_id,
                    'sessionId': confirmed.get('session_id') or session_id}
        if run.lines == 0 and run.exit_code and run.exit_code > 0:
            # An error exit before printing anything: Claude Code refused before the turn began.
            with self._lock:
                self._runs.pop(request_id, None)
            raise NotAccepted('Claude Code exited with code ' + str(run.exit_code) + ' before starting the turn' + run.reason())
        raise RuntimeError('Claude Code exited with code ' + str(run.exit_code) + ' without confirming the turn' + run.reason())

    def observe(self, agent, request_id, receipt):
        """Our own process's result when we have it; after a restart, the session transcript."""
        with self._lock:
            run = self._runs.get(request_id)
        if run is not None:
            return self._observe_run(run)
        return self._observe_transcript(agent, request_id, receipt or {})

    def close(self):
        """Stop the delivery processes this adapter started. Their turns are interrupted."""
        with self._lock:
            running = [run for run in self._runs.values() if not run.done.is_set()]
        for run in running:
            with contextlib.suppress(OSError):
                run.proc.terminate()
        for run in running:
            try:
                run.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                run.proc.kill()
                run.proc.wait()
            run.done.wait(timeout=3)

    # ---- observing ----------------------------------------------------------------
    @staticmethod
    def _observe_run(run):
        found = {'observedTurnId': run.request_id, 'sessionId': run.session_ids()[-1]}
        result = run.result
        if result is not None:
            found['resultSubtype'] = result.get('subtype')
            if result.get('is_error'):
                why = result.get('result') or result.get('subtype') or 'no reason given'
                return {'status': 'FAILED', 'detail': 'Claude Code reported an error: ' + cards.plain(why, 300),
                        'receipt': found}
            return {'status': 'RETURNED', 'detail': 'Claude Code finished the turn.', 'receipt': found}
        if not run.done.is_set():
            return {'status': 'RUNNING', 'detail': 'Claude Code is working on the turn.', 'receipt': found}
        found['exitCode'] = run.exit_code
        if run.exit_code is not None and run.exit_code < 0:
            found['turnOutcome'] = 'interrupted'
            return {'status': 'FAILED', 'receipt': found,
                    'detail': 'The Claude Code process was stopped (signal ' + str(-run.exit_code) + ') before the turn finished.'}
        return {'status': 'FAILED', 'receipt': found,
                'detail': 'Claude Code exited with code ' + str(run.exit_code) + ' without a result' + run.reason()}

    def _observe_transcript(self, agent, request_id, receipt):
        session_ids = []
        for session_id in (agent.get('endpoint'), receipt.get('sessionId')):
            if session_id and session_id not in session_ids:
                session_ids.append(session_id)
        paths = [path for session_id in session_ids for path in self.transcripts(session_id)]
        if not paths:
            return {'status': None, 'detail': 'No Claude Code transcript was found for this session, so the delivery '
                                              'cannot be checked yet.'}
        candidate, complete = None, True
        for path in paths:
            try:
                scan = _scan_transcript(path, request_id)
                modified = path.stat().st_mtime
            except OSError:
                complete = False  # an unreadable transcript means the search proves nothing
                continue
            if scan['found']:
                return _from_transcript(scan['ending'], modified, request_id)
            candidate = candidate or scan['candidate']
        if complete and candidate is None:
            detail = 'The session transcript was searched and this delivery is not in it.'
        elif complete:
            detail = 'No delivered turn found; transcript entry ' + str(candidate) + ' only mentions this handoff.'
        else:
            detail = 'A session transcript could not be read; the search is incomplete.'
        return {'status': None, 'detail': detail,
                'receipt': {'historySearch': {'exhausted': complete, 'candidate': candidate, 'turnId': None}}}

    # ---- bookkeeping --------------------------------------------------------------
    def _latest_run(self, session_id):
        with self._lock:
            for run in reversed(list(self._runs.values())):
                if run.session_id == session_id:
                    return run
        return None

    def _last_modified(self, session_ids):
        times = []
        for session_id in session_ids:
            for path in self.transcripts(session_id):
                with contextlib.suppress(OSError):
                    times.append(path.stat().st_mtime)
        return max(times) if times else None

    def _last_ending(self, session_ids):
        """How the newest transcript's last turn stands: 'open', 'finished', 'failed', 'stopped' or None."""
        paths = [path for session_id in session_ids for path in self.transcripts(session_id)]
        if not paths:
            return None
        try:
            newest = max(paths, key=lambda path: path.stat().st_mtime)
            return _tail_ending(newest)
        except OSError:
            return None

    def _forget_old_runs(self):
        """Keep every running process and the most recent finished ones. Call with the lock held."""
        finished = [request_id for request_id, run in self._runs.items() if run.done.is_set()]
        for request_id in finished[:max(0, len(finished) - KEEP_FINISHED)]:
            del self._runs[request_id]


# ---- transcript reading -----------------------------------------------------------
def _scan_transcript(path, request_id) -> dict:
    """Find the delivery in one transcript and, if its turn has ended, how.

    ending is 'finished' (the model ended its turn), 'error' (an API error ended it),
    'interrupted' (a new prompt arrived before it finished) or None (not ended yet).
    """
    found, candidate, ending = False, None, None
    with open(path, 'rb') as f:
        for raw in f:
            try:
                entry = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(entry, dict) or entry.get('isSidechain') or entry.get('isMeta'):
                continue
            kind = entry.get('type')
            texts = _prompt_texts(entry) if kind == 'user' else []
            if not found:
                if any(cards.marker_matches(text, request_id) for text in texts):
                    found = True
                elif candidate is None and any(request_id in text for text in texts):
                    candidate = entry.get('uuid') or 'unnamed entry'
                continue
            if kind == 'assistant':
                message = entry.get('message') if isinstance(entry.get('message'), dict) else {}
                if entry.get('isApiErrorMessage'):
                    ending = 'error'
                elif message.get('stop_reason') in FINISHED_STOPS:
                    ending = 'finished'
                else:
                    ending = None  # still working, for example a tool call in flight
            elif texts:
                return {'found': True, 'candidate': None, 'ending': ending or 'interrupted'}
    return {'found': found, 'candidate': candidate, 'ending': ending}


def _tail_ending(path):
    """Read the end of a transcript and say whether its last main-chain turn has ended.

    'finished' after end_turn (or stop_sequence), 'failed' after an API error, 'stopped'
    after an interruption, 'open' while a tool call or an unanswered prompt or tool result
    is last, None when the end of the file shows no turn at all. Local slash-command
    entries are not turns and are skipped.
    """
    with open(path, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - TAIL_BYTES))
        data = f.read()
    lines = data.split(b'\n')
    if size > TAIL_BYTES:
        lines = lines[1:]  # the first line may be cut in half
    for raw in reversed(lines):
        try:
            entry = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(entry, dict) or entry.get('isSidechain') or entry.get('isMeta'):
            continue
        kind = entry.get('type')
        if kind == 'assistant':
            message = entry.get('message') if isinstance(entry.get('message'), dict) else {}
            if entry.get('isApiErrorMessage'):
                return 'failed'
            return 'finished' if message.get('stop_reason') in FINISHED_STOPS else 'open'
        if kind != 'user':
            continue
        texts = _prompt_texts(entry)
        if texts and all(text.lstrip().startswith(LOCAL_COMMAND_TAGS) for text in texts):
            continue  # a local slash command and its output; the model does not answer those
        if any(text.lstrip().startswith(INTERRUPTED_PREFIX) for text in texts):
            return 'stopped'
        return 'open'  # a prompt or a tool result the model has not answered yet
    return None


def _from_transcript(ending, modified, request_id) -> dict:
    found = {'observedTurnId': request_id, 'foundIn': 'transcript'}
    if ending == 'finished':
        return {'status': 'RETURNED', 'detail': 'Found in the session transcript; the turn finished.', 'receipt': found}
    if ending == 'error':
        return {'status': 'FAILED', 'detail': 'The session transcript shows an API error ended the turn.', 'receipt': found}
    if ending is None and time.time() - modified < RECENT_SECONDS:
        return {'status': 'RUNNING', 'detail': 'Found in the session transcript; the turn is still being written.',
                'receipt': found}
    found['turnOutcome'] = 'interrupted'
    detail = ('A new prompt arrived before the turn finished.' if ending == 'interrupted' else
              'The turn went quiet without finishing; its delivery process probably stopped with the engine.')
    return {'status': 'FAILED', 'detail': detail, 'receipt': found}


def _prompt_texts(entry) -> list:
    """The text a person (or a delivery) typed; tool results and meta entries do not count."""
    message = entry.get('message')
    content = message.get('content') if isinstance(message, dict) else None
    if isinstance(content, str):
        return [content]
    if not isinstance(content, list):
        return []
    return [part['text'] for part in content
            if isinstance(part, dict) and part.get('type') == 'text' and isinstance(part.get('text'), str)]


# ---- small helpers ----------------------------------------------------------------
def _argv(value, setting) -> list:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)) and value and all(isinstance(v, str) and v for v in value):
        return list(value)
    raise ValueError(setting + ' must be a path or a list of strings')


def _runnable(program) -> bool:
    if os.sep in program:
        return os.path.isfile(program) and os.access(program, os.X_OK)
    return shutil.which(program) is not None
