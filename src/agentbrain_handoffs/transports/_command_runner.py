"""Runs one command-agent turn on its own and writes down how it ended.

The command adapter starts this file with the interpreter (``python -I``) in a new
session, so the turn does not depend on the engine process: ``handoffs tick`` can exit
and ``handoffs serve`` can restart while the command keeps running, and every engine
that reads the run record later sees the same outcome.

    python -I _command_runner.py RECORD TIMEOUT TEXT_FILE -- ARGV...

RECORD is the run's JSON record, which the adapter created. While this runner lives it
holds an exclusive lock on RECORD's ``.lock`` sibling, so a reader can tell a live run
from one whose runner was killed. On its first stdout line it answers the adapter with
``{"pid": N}`` or ``{"error": "..."}``, then stops writing to stdout.

Standard library only, and nothing from the package itself, so it runs under ``-I``.
"""
import collections
import contextlib
import fcntl
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time

TAIL_LINES = 20
MAX_LINE = 500
STOP_GRACE = 5
SAVE_EVERY = 1.0  # seconds between output-tail updates while the command runs


def load(path):
    try:
        with open(path, encoding='utf-8') as f:
            record = json.load(f)
        return record if isinstance(record, dict) else {}
    except (OSError, ValueError):
        return {}


def save(path, record):
    """Replace the record atomically, so a reader never sees half a file."""
    folder = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.run-', suffix='.tmp', dir=folder)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(record, f)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def answer(message):
    """The one line the adapter waits for; then stdout is closed for good."""
    with contextlib.suppress(OSError):
        os.write(1, (json.dumps(message) + '\n').encode('utf-8'))
    with contextlib.suppress(OSError):
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        os.close(devnull)


def main(argv):
    if len(argv) < 5 or argv[3] != '--':
        return 2
    record_path, timeout, text_file, command = argv[0], float(argv[1]), argv[2], argv[4:]
    lock = open(record_path[:-len('.json')] + '.lock', 'a')  # held until this process ends
    fcntl.flock(lock, fcntl.LOCK_EX)
    record = load(record_path)
    record['runnerPid'] = os.getpid()
    try:
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, start_new_session=True)
    except (OSError, ValueError) as error:
        with contextlib.suppress(OSError):
            os.unlink(text_file)
        record.update(status='not-started', error=str(error)[:300], ended=time.time())
        save(record_path, record)
        answer({'error': str(error)[:300]})
        return 127
    record.update(status='open', pid=child.pid)
    save(record_path, record)
    answer({'pid': child.pid})

    state = {'timedOut': False}

    def stop():
        """Ask the command's process group to end, then insist after a grace period."""
        state['timedOut'] = True
        for sig in (signal.SIGTERM, signal.SIGKILL):
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(child.pid, sig)
            try:
                child.wait(timeout=STOP_GRACE)
                return
            except subprocess.TimeoutExpired:
                continue

    timer = threading.Timer(timeout, stop)
    timer.daemon = True
    timer.start()
    tail = collections.deque(maxlen=TAIL_LINES)
    saved = time.monotonic()
    try:
        for chunk in iter(lambda: child.stdout.readline(4 * MAX_LINE), b''):
            tail.append(chunk.decode('utf-8', 'replace').rstrip('\r\n')[:MAX_LINE])
            if time.monotonic() - saved >= SAVE_EVERY:
                record['outputTail'] = list(tail)
                with contextlib.suppress(OSError):
                    save(record_path, record)
                saved = time.monotonic()
        code = child.wait()
    finally:
        timer.cancel()
        child.stdout.close()
        with contextlib.suppress(OSError):
            os.unlink(text_file)
    if state['timedOut'] or code < 0:
        status = 'stopped'  # stopped for its time limit, or by a signal
    else:
        status = 'finished' if code == 0 else 'failed'
    record.update(status=status, exitCode=code, timedOut=state['timedOut'], outputTail=list(tail), ended=time.time())
    save(record_path, record)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
