"""Deliver handoffs to any agent you can start from a command line or reach over HTTP.

Command mode (agent setting ``command``): every delivery runs your argv once, with
no shell, after writing the handoff text to a private temp file. Placeholders in
any argument are filled in:

    {text_file}   path of the file holding the handoff text
    {request_id}  the delivery's unique request id
    {endpoint}    the agent's endpoint
    {cwd}         the agent's working directory

The same values are also in the environment as HANDOFFS_TEXT_FILE,
HANDOFFS_REQUEST_ID and HANDOFFS_AGENT_ID. HANDOFFS_AGENT is set to the
recipient, so ``handoffs return ID --as ...`` inside the turn acts as that agent
and never as whoever started the engine, and HANDOFFS_DB names the engine's
database when it is known. Exit code 0 means the work turn finished; anything
else means it failed. The run is stopped after ``timeout`` seconds (default
3600). Example agent settings:

    {"command": ["python3", "my_agent.py", "--prompt-file", "{text_file}"], "timeout": 900}

Each run is supervised by a small runner process of its own (``_command_runner.py``)
that records the outcome in a run record: ``<db>.command-runs/`` next to the
database when the adapter is given the store, otherwise a private temporary folder.
So a run does not depend on the engine that started it: a one-shot ``handoffs
tick`` may exit and ``handoffs serve`` may restart, and the next engine still sees
how the run ended. A run whose runner was killed (a reboot, say) is reported as
interrupted, never guessed as finished.

HTTP mode (agent setting ``url``, which takes precedence over ``command``): each
delivery is one POST of ``{"request_id", "text", "agent"}``. A 2xx answer means
the turn was accepted. Without ``status_url`` it counts as returned at once. With
``status_url`` (``{request_id}`` is filled in, or it is added as a query
parameter) a GET must answer ``{"status": ...}`` with one of: accepted, running,
returned, failed or stopped. A 404 there means the endpoint has no such request.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import select
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from ..transport import NotAccepted, Transport

DEFAULT_TIMEOUT = 3600
DEFAULT_REQUEST_TIMEOUT = 30
MAX_RUNS = 500           # settled run records kept per folder
HANDSHAKE_TIMEOUT = 30   # seconds start() waits for the runner to report the command started
RUNNER = Path(__file__).with_name('_command_runner.py')
PLACEHOLDERS = ('text_file', 'request_id', 'endpoint', 'cwd')
LOOPBACK = ('127.0.0.1', 'localhost', '::1')

# What an HTTP status endpoint may answer, and what the engine and activity see.
REMOTE_STATUS = {
    'accepted': ('ACCEPTED', 'open'), 'queued': ('ACCEPTED', 'open'),
    'running': ('RUNNING', 'open'), 'in_progress': ('RUNNING', 'open'),
    'returned': ('RETURNED', 'finished'), 'finished': ('RETURNED', 'finished'), 'completed': ('RETURNED', 'finished'),
    'failed': ('FAILED', 'failed'), 'error': ('FAILED', 'failed'),
    'stopped': ('FAILED', 'stopped'), 'interrupted': ('FAILED', 'stopped'), 'cancelled': ('FAILED', 'stopped'),
}
TURN_OUTCOMES = {'finished': 'completed', 'failed': 'failed', 'stopped': 'interrupted'}


class CommandTransport(Transport):
    """Adapter for provider 'command': runs a command or posts to a URL per delivery.

    store: optional Store. With it, run records live next to the database, so every
        engine, page and CLI process on that database sees the same runs, and spawned
        turns get HANDOFFS_DB.
    state_dir: where run records live instead (tests, custom layouts).
    """

    name = 'command'

    def __init__(self, store=None, state_dir=None):
        self._lock = threading.Lock()
        self._posted = {}                         # agent id -> latest accepted HTTP request id
        if state_dir is None and store is not None:
            state_dir = Path(store.path).with_name(Path(store.path).name + '.command-runs')
        self._state_dir = Path(state_dir).absolute() if state_dir is not None else None
        self._db = str(Path(store.path).absolute()) if store is not None else None

    # ---- the adapter contract ------------------------------------------------------
    def activity(self, agent: dict) -> dict:
        """'open' while this agent's command (or accepted HTTP request) is still running."""
        try:
            config = settings(agent)
        except ValueError:
            return {'turnStatus': 'finished', 'turnId': None}  # prepare() reports the setting problem
        if config['url']:
            return self._remote_activity(agent, config)
        folder = self._folder(create=False)
        record = self._latest(folder, agent.get('id')) if folder else None
        if record is None:
            return {'turnStatus': 'finished', 'turnId': None}
        state = self._state(folder, record)
        if state in ('open', 'finished', 'failed', 'stopped'):
            return {'turnStatus': state, 'turnId': record.get('requestId')}
        if state == 'lost':
            return {'turnStatus': 'stopped', 'turnId': record.get('requestId')}
        return {'turnStatus': 'finished', 'turnId': None}  # it never started

    @contextlib.contextmanager
    def prepare(self, agent: dict):
        """Check the agent's settings before anything is written or run."""
        yield {'agent': agent, 'config': settings(agent)}

    def start(self, prepared: dict, text: str, request_id: str) -> dict:
        agent, config = prepared['agent'], prepared['config']
        if config['url']:
            return self._post(agent, config, text, request_id)
        return self._run(agent, config, text, request_id)

    def observe(self, agent: dict, request_id: str, receipt: dict) -> dict:
        try:
            config = settings(agent)
        except ValueError as error:
            return {'status': None, 'detail': str(error), 'receipt': {}}
        if config['url']:
            return self._remote_observe(config, request_id, receipt or {})
        folder, record = self._find(agent, request_id, receipt or {})
        if record is None or record.get('status') == 'not-started':
            return {'status': None, 'receipt': {},
                    'detail': 'No record of that command run was found; the outcome is unknown and nothing is guessed.'}
        state = self._state(folder, record)
        found = {'turnId': request_id, 'pid': record.get('pid'), 'outputTail': list(record.get('outputTail') or [])}
        if state == 'open':
            return {'status': 'RUNNING', 'detail': 'The command is running.', 'receipt': found}
        if state == 'lost':
            found['turnOutcome'] = 'interrupted'
            return {'status': 'FAILED', 'receipt': found,
                    'detail': 'The command\'s runner ended before it recorded a result (for example the computer '
                              'restarted), so the run counts as interrupted.'}
        found.update(exitCode=record.get('exitCode'), turnOutcome=TURN_OUTCOMES[state])
        if record.get('timedOut'):
            found['timedOut'] = True
            return {'status': 'FAILED', 'detail': 'The command ran past its time limit and was stopped.', 'receipt': found}
        if state == 'stopped':
            return {'status': 'FAILED', 'detail': 'The command was stopped by a signal.', 'receipt': found}
        if state == 'failed':
            return {'status': 'FAILED', 'detail': 'The command exited with code %s.' % record.get('exitCode'),
                    'receipt': found}
        return {'status': 'RETURNED', 'detail': 'The command finished (exit code 0).', 'receipt': found}

    # ---- command mode: run records -------------------------------------------------
    def _folder(self, create=True):
        """The folder of run records (None when nothing was ever recorded here)."""
        with self._lock:
            if self._state_dir is None:
                if not create:
                    return None
                self._state_dir = Path(tempfile.mkdtemp(prefix='handoffs-command-runs-'))
            folder = self._state_dir
        if folder.is_symlink():
            raise ValueError('The command run folder must not be a symlink: ' + folder.name)
        if create:
            folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        elif not folder.is_dir():
            return None
        return folder

    @staticmethod
    def _names(agent_id, request_id=None):
        """File names from hashes, so any agent or request id is a safe file name."""
        agent_key = hashlib.sha256(str(agent_id).encode('utf-8')).hexdigest()[:16]
        if request_id is None:
            return agent_key
        return agent_key + '-' + hashlib.sha256(str(request_id).encode('utf-8')).hexdigest()[:24] + '.json'

    def _find(self, agent, request_id, receipt):
        """(folder, record) for this agent's run of this request, or (None, None)."""
        name = self._names(agent.get('id'), request_id)
        folder = self._folder(create=False)
        places = [folder / name] if folder else []
        # A record another adapter instance made (an engine without the store, say)
        # is found through the receipt; it must carry exactly this run's file name.
        remembered = receipt.get('runState')
        if isinstance(remembered, str) and os.path.isabs(remembered) and os.path.basename(remembered) == name:
            places.append(Path(remembered))
        for path in places:
            record = _load(path)
            if record and record.get('requestId') == request_id and record.get('agentId') == agent.get('id'):
                return path.parent, record
        return None, None

    def _latest(self, folder, agent_id):
        pointer = folder / (self._names(agent_id) + '.latest')
        try:
            name = pointer.read_text(encoding='utf-8').strip()
        except OSError:
            return None
        if not name or os.path.basename(name) != name:
            return None
        record = _load(folder / name)
        return record if record and record.get('agentId') == agent_id else None

    def _state(self, folder, record):
        """open, finished, failed, stopped, not-started, or lost (its runner died mid-run)."""
        status = record.get('status')
        if status not in ('starting', 'open'):
            return status if status in ('finished', 'failed', 'stopped', 'not-started') else 'lost'
        path = folder / self._names(record.get('agentId'), record.get('requestId'))
        if _runner_alive(path):
            return 'open'
        again = _load(path) or record  # it may have ended between the two reads
        if again.get('status') not in ('starting', 'open'):
            return self._state(folder, again)
        started = again.get('started')
        if again.get('status') == 'starting' and isinstance(started, (int, float)) and \
                time.time() - started < HANDSHAKE_TIMEOUT + 5:
            return 'open'  # the runner is being started right now
        return 'lost'

    def _run(self, agent, config, text, request_id):
        """Start a runner for the command and return once the command is running.

        Starting happens here, not in the background, so a command that cannot
        start at all is reported as not accepted instead of as an unknown result.
        """
        folder = self._folder()
        agent_id = agent.get('id')
        name = self._names(agent_id, request_id)
        path = folder / name
        with open(folder / (self._names(agent_id) + '.lock'), 'a') as agent_lock:
            fcntl.flock(agent_lock, fcntl.LOCK_EX)  # one start at a time per agent, across processes
            existing = _load(path)
            if existing.get('requestId') == request_id and existing.get('status') != 'not-started':
                return self._receipt(existing, path)  # one run per request, ever
            latest = self._latest(folder, agent_id)
            if latest is not None and self._state(folder, latest) == 'open':
                raise NotAccepted("The agent's previous command is still running.")
            if not sys.executable:
                raise NotAccepted('No Python interpreter is available to supervise the command.')
            fd, text_file = tempfile.mkstemp(prefix='handoff-', suffix='.txt')  # readable by this user only
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(text)
            values = {'text_file': text_file, 'request_id': request_id,
                      'endpoint': str(agent.get('endpoint') or ''), 'cwd': str(agent.get('cwd') or '')}
            argv = [fill(arg, values) for arg in config['command']]
            env = dict(os.environ, HANDOFFS_TEXT_FILE=text_file, HANDOFFS_REQUEST_ID=request_id,
                       HANDOFFS_AGENT_ID=str(agent_id or ''), HANDOFFS_AGENT=str(agent_id or ''))
            if self._db:
                env['HANDOFFS_DB'] = self._db
            record = {'requestId': request_id, 'agentId': agent_id, 'status': 'starting', 'started': time.time()}
            _save(path, record)
            _save(folder / (self._names(agent_id) + '.latest'), name, raw=True)
            try:
                runner = subprocess.Popen([sys.executable, '-I', str(RUNNER), str(path), repr(float(config['timeout'])),
                                           text_file, '--', *argv],
                                          cwd=agent.get('cwd') or None, env=env, stdin=subprocess.DEVNULL,
                                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True)
            except (OSError, ValueError) as error:
                with contextlib.suppress(OSError):
                    os.unlink(text_file)
                _save(path, {**record, 'status': 'not-started', 'error': str(error)[:300]})
                raise NotAccepted('The command could not start: ' + str(error)) from error
            # Collect the runner's exit status whenever it ends, so it never lingers as a zombie.
            threading.Thread(target=runner.wait, name='handoff-runner-' + request_id[:8], daemon=True).start()
            answer = _handshake(runner)
            record = _load(path) or record
            if answer.get('error') or record.get('status') == 'not-started':
                raise NotAccepted('The command could not start: ' + str(answer.get('error') or record.get('error')))
            if 'pid' not in answer and record.get('status') != 'open':
                raise RuntimeError('The command runner did not confirm the start within %d s; the run is '
                                   'followed by observation.' % HANDSHAKE_TIMEOUT)
            self._forget_old_runs(folder)
            return self._receipt(record, path)

    @staticmethod
    def _receipt(record, path):
        return {'turnId': record['requestId'], 'clientUserMessageId': record['requestId'], 'pid': record.get('pid'),
                'runState': str(path)}

    def _forget_old_runs(self, folder):
        """Keep every open run and the most recent settled ones."""
        records = sorted(folder.glob('*.json'), key=lambda p: _mtime(p))
        extra = len(records) - MAX_RUNS
        for path in records:
            if extra <= 0:
                break
            record = _load(path)
            if record.get('status') in ('starting', 'open') and self._state(folder, record) == 'open':
                continue
            for stale in (path, path.with_suffix('.lock')):
                with contextlib.suppress(OSError):
                    stale.unlink()
            extra -= 1

    # ---- HTTP mode -----------------------------------------------------------------
    def _post(self, agent, config, text, request_id):
        payload = {'request_id': request_id, 'text': text,
                   'agent': {k: agent.get(k) for k in ('id', 'name', 'provider', 'endpoint', 'cwd')}}
        request = urllib.request.Request(config['url'], data=json.dumps(payload).encode('utf-8'), method='POST',
                                         headers={'Content-Type': 'application/json'})
        try:
            with _opener(config['url']).open(request, timeout=config['request_timeout']) as response:
                code = response.status
        except urllib.error.HTTPError as error:
            if error.code < 500:
                # The endpoint answered and refused (a redirect never carries the POST along).
                raise NotAccepted('The agent endpoint refused the handoff (HTTP %d).' % error.code) from error
            raise RuntimeError('The agent endpoint answered HTTP %d, so it may or may not have taken the handoff.'
                               % error.code) from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, ConnectionRefusedError):
                raise NotAccepted('Nothing is listening at the agent endpoint (connection refused).') from error
            raise
        with self._lock:
            self._posted[agent.get('id')] = request_id
        return {'turnId': request_id, 'clientUserMessageId': request_id, 'httpStatus': code}

    def _remote_activity(self, agent, config):
        with self._lock:
            request_id = self._posted.get(agent.get('id'))
        if request_id is None or not config['status_url']:
            return {'turnStatus': 'finished', 'turnId': request_id}
        answer = remote_status(config, request_id)
        if answer.get('status') not in REMOTE_STATUS:
            return {'turnStatus': 'unknown', 'turnId': request_id}
        return {'turnStatus': REMOTE_STATUS[answer['status']][1], 'turnId': request_id}

    def _remote_observe(self, config, request_id, receipt):
        accepted = receipt.get('turnId') == request_id
        if not config['status_url']:
            if accepted:
                return {'status': 'RETURNED', 'receipt': {'turnId': request_id},
                        'detail': 'The agent endpoint accepted the handoff; there is no status URL to follow.'}
            return {'status': None, 'receipt': {},
                    'detail': 'No accepted request is on record and there is no status URL to ask.'}
        answer = remote_status(config, request_id)
        if answer.get('missing'):
            return {'status': None, 'detail': 'The status URL has no record of this request.',
                    'receipt': {'historySearch': {'exhausted': True, 'candidate': None, 'turnId': None}}}
        state = answer.get('status')
        if state not in REMOTE_STATUS:
            return {'status': None, 'receipt': {},
                    'detail': answer.get('error') or 'The status URL answered an unrecognized status: ' + str(state)[:60]}
        observed, turn_status = REMOTE_STATUS[state]
        found = {'turnId': request_id, 'turnStatus': turn_status}
        if turn_status in TURN_OUTCOMES:
            found['turnOutcome'] = TURN_OUTCOMES[turn_status]
        return {'status': observed, 'detail': 'The status URL reports: ' + state + '.', 'receipt': found}


# ---- run records ---------------------------------------------------------------------
def _load(path) -> dict:
    try:
        with open(path, encoding='utf-8') as f:
            record = json.load(f)
        return record if isinstance(record, dict) else {}
    except (OSError, ValueError):
        return {}


def _save(path, value, raw=False):
    """Write atomically (temp file, then rename), so a reader never sees half a record."""
    path = Path(path)
    fd, tmp = tempfile.mkstemp(prefix='.run-', suffix='.tmp', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(value if raw else json.dumps(value))
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _mtime(path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _runner_alive(record_path) -> bool:
    """A live runner holds the run's lock for as long as it lives; the kernel drops it on exit."""
    lock_path = Path(record_path).with_suffix('.lock')
    try:
        with open(lock_path, 'a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
            return False
    except OSError:
        return False


def _handshake(runner) -> dict:
    """The runner's one-line answer ({'pid': N} or {'error': ...}), or {} if none came in time."""
    line = b''
    try:
        ready, _, _ = select.select([runner.stdout], [], [], HANDSHAKE_TIMEOUT)
        if ready:
            line = runner.stdout.readline(65536)
    except (OSError, ValueError):
        line = b''
    finally:
        with contextlib.suppress(OSError):
            runner.stdout.close()
    try:
        answer = json.loads(line.decode('utf-8', 'replace'))
    except ValueError:
        return {}
    return answer if isinstance(answer, dict) else {}


# ---- helpers -------------------------------------------------------------------------
def settings(agent: dict) -> dict:
    """The agent's command or HTTP settings, checked, with defaults filled in.

    Raises ValueError with a plain explanation, which the engine shows on the
    handoff, when the settings cannot work.
    """
    raw = (agent or {}).get('settings') or {}
    url, status_url = raw.get('url') or None, raw.get('status_url') or None  # empty means not set
    for key, value in (('url', url), ('status_url', status_url)):
        if value is not None and (not isinstance(value, str) or urllib.parse.urlsplit(value).scheme not in ('http', 'https')):
            raise ValueError('The ' + key + ' setting must be an http:// or https:// address.')
    command = raw.get('command')
    if isinstance(command, str):
        command = shlex.split(command)  # split like a shell would, but never run through one
    if not url and (not isinstance(command, list) or not command or not all(isinstance(a, str) and a for a in command)):
        raise ValueError('A command agent needs a "command" setting (a list of arguments, such as '
                         '["python3", "agent.py", "{text_file}"]) or a "url" setting.')
    return {'command': command, 'url': url, 'status_url': status_url,
            'timeout': _seconds(raw, 'timeout', DEFAULT_TIMEOUT),
            'request_timeout': _seconds(raw, 'request_timeout', DEFAULT_REQUEST_TIMEOUT)}


def _seconds(raw, key, default):
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError('The ' + key + ' setting must be a number of seconds above zero.')
    return value


def fill(arg: str, values: dict) -> str:
    """Replace only the known {placeholders}; other braces (JSON, say) stay as written."""
    for key in PLACEHOLDERS:
        arg = arg.replace('{' + key + '}', values[key])
    return arg


def remote_status(config, request_id) -> dict:
    """GET the status URL. Returns {'status': ...}, {'missing': True} or {'error': ...}."""
    template = config['status_url']
    quoted = urllib.parse.quote(request_id, safe='')
    if '{request_id}' in template:
        url = template.replace('{request_id}', quoted)
    else:
        url = template + ('&' if urllib.parse.urlsplit(template).query else '?') + 'request_id=' + quoted
    try:
        with _opener(url).open(url, timeout=config['request_timeout']) as response:
            answer = json.loads(response.read(65536).decode('utf-8'))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return {'missing': True}
        return {'error': 'The status URL answered HTTP %d.' % error.code}
    except (OSError, ValueError) as error:
        return {'error': 'The status URL could not be read: ' + str(error)[:200]}
    state = answer.get('status') if isinstance(answer, dict) else None
    return {'status': str(state).strip().lower()} if state else {'error': 'The status URL gave no status.'}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirected POST would arrive as a GET without its body, so never follow one."""

    def redirect_request(self, *args, **kwargs):
        return None


def _opener(url):
    """Loopback addresses never go through a proxy; other addresses honor the usual proxy settings."""
    handlers = [_NoRedirect()]
    if urllib.parse.urlsplit(url).hostname in LOOPBACK:
        handlers.append(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(*handlers)
