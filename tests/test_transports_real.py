"""Codex and Claude Code adapters, driven only by fake protocol executables.

Nothing in this module starts a real `codex` or `claude` binary. Each fake is a
Python script that speaks the documented protocol (JSON-RPC app-server for Codex,
stream-json headless mode for Claude Code) and is pointed at through `binary`,
agent settings, or the matching environment variable.
"""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from agentbrain_handoffs import cards
from agentbrain_handoffs.engine import Engine
from agentbrain_handoffs.store import Store
from agentbrain_handoffs.transport import NotAccepted, Router
from agentbrain_handoffs.transports.claude_code import ClaudeCodeTransport
from agentbrain_handoffs.transports.codex import CodexTransport
from agentbrain_handoffs.transports.demo import DemoTransport

# ---- fake Codex app-server (JSON-RPC 2.0, one JSON object per line) ------------------
FAKE_CODEX = r'''
import fcntl, json, os, sys

STATE_PATH = os.environ['FAKE_CODEX_STATE']


def send(message):
    sys.stdout.write(json.dumps(message) + '\n')
    sys.stdout.flush()


def locked_state():
    f = open(STATE_PATH, 'r+')
    fcntl.flock(f, fcntl.LOCK_EX)
    state = json.load(f)
    return f, state


def save_state(f, state):
    f.seek(0)
    f.truncate()
    json.dump(state, f)
    f.flush()
    os.fsync(f.fileno())
    f.close()


def main():
    f, state = locked_state()
    state.setdefault('pids', []).append(os.getpid())
    state['argv'] = sys.argv
    state['alive'] = True
    state['env'] = {k: os.environ.get(k) for k in ('HANDOFFS_AGENT', 'HANDOFFS_DB')}
    save_state(f, state)

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if not isinstance(msg, dict):
            continue
        method = msg.get('method')
        params = msg.get('params') if isinstance(msg.get('params'), dict) else {}
        req_id = msg.get('id')
        f, state = locked_state()
        state.setdefault('requests', []).append({
            'method': method, 'id': req_id, 'params': params, 'pid': os.getpid(),
        })
        reply = None
        if req_id is None:
            if method == 'initialized':
                state['initialized'] = True
        elif method in (state.get('errors') or {}):
            err = state['errors'][method]
            if state.get('consume_errors'):
                state['errors'].pop(method, None)
            reply = {'jsonrpc': '2.0', 'id': req_id, 'error': err}
        elif method == 'initialize':
            state['initialize_count'] = state.get('initialize_count', 0) + 1
            state['clientInfo'] = params.get('clientInfo')
            reply = {'jsonrpc': '2.0', 'id': req_id, 'result': {}}
        elif method == 'thread/read':
            thread = dict(state.get('thread') or {})
            thread.setdefault('id', params.get('threadId'))
            thread.setdefault('status', {'type': 'idle'})
            reply = {'jsonrpc': '2.0', 'id': req_id, 'result': {'thread': thread}}
        elif method == 'thread/resume':
            thread = state.setdefault('thread', {})
            thread['id'] = params.get('threadId')
            thread['status'] = {'type': 'idle'}
            state['resumed'] = True
            reply = {'jsonrpc': '2.0', 'id': req_id, 'result': {'thread': thread}}
        elif method == 'thread/turns/list':
            turns = list(state.get('turns') or [])
            limit = int(params.get('limit') or 10)
            cursor = params.get('cursor')
            offset = int(cursor) if cursor not in (None, '') else 0
            page = turns[offset:offset + limit]
            nxt = offset + limit
            result = {'data': page}
            if state.get('always_cursor') or nxt < len(turns):
                result['nextCursor'] = str(nxt)
            reply = {'jsonrpc': '2.0', 'id': req_id, 'result': result}
        elif method == 'turn/start':
            if state.get('start_error'):
                err = dict(state['start_error'])
                if state.get('consume_errors'):
                    state['start_error'] = None
                reply = {'jsonrpc': '2.0', 'id': req_id, 'error': err}
            else:
                text = ''
                for item in params.get('input') or []:
                    if isinstance(item, dict) and item.get('type') == 'text':
                        text = item.get('text') or ''
                turn_id = state.get('next_turn_id', 'turn-1')
                turn = {
                    'id': turn_id,
                    'status': state.get('start_status', 'inProgress'),
                    'items': [{'type': 'userMessage', 'content': [{'type': 'text', 'text': text}]}],
                }
                state['last_start'] = params
                state.setdefault('turns', []).insert(0, turn)
                thread = state.setdefault('thread', {})
                thread.setdefault('id', params.get('threadId'))
                if state.get('start_sets_active', True):
                    thread['status'] = {'type': 'active'}
                reply = {'jsonrpc': '2.0', 'id': req_id, 'result': {'turn': turn}}
        else:
            reply = {'jsonrpc': '2.0', 'id': req_id,
                     'error': {'code': -32601, 'message': 'Unknown method ' + str(method)}}
        save_state(f, state)
        if reply is not None:
            send(reply)

    f, state = locked_state()
    state['alive'] = False
    save_state(f, state)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        sys.stderr.write('fake-codex failed: %s\n' % e)
        sys.stderr.flush()
        sys.exit(1)
'''

FAKE_CLAUDE = r'''
import json, os, sys, time

STATE_PATH = os.environ.get('FAKE_CLAUDE_STATE')
mode = os.environ.get('FAKE_CLAUDE_MODE', 'ok')
release = os.environ.get('FAKE_CLAUDE_RELEASE')


def record(**fields):
    if not STATE_PATH:
        return
    state = {}
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH) as f:
                state = json.load(f)
        except ValueError:
            state = {}
    state.update(fields)
    tmp = STATE_PATH + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_PATH)


def emit(event):
    sys.stdout.write(json.dumps(event) + '\n')
    sys.stdout.flush()


def main():
    session = ''
    argv = sys.argv[1:]
    if '--resume' in argv:
        session = argv[argv.index('--resume') + 1]
    record(pid=os.getpid(), argv=sys.argv, session=session, mode=mode,
           env={k: os.environ.get(k) for k in ('HANDOFFS_AGENT', 'HANDOFFS_DB')})

    if mode == 'refuse':
        sys.stderr.write('session not found\n')
        sys.stderr.flush()
        sys.exit(2)

    if mode == 'hang-silent':
        time.sleep(3600)
        return

    emit({'type': 'system', 'subtype': 'init', 'session_id': session})
    prompt = sys.stdin.read()
    record(prompt=prompt)

    if mode == 'run':
        deadline = time.time() + 30
        while time.time() < deadline:
            if release and os.path.exists(release):
                break
            time.sleep(0.02)

    is_error = mode == 'error'
    emit({
        'type': 'result',
        'subtype': 'error' if is_error else 'success',
        'is_error': is_error,
        'session_id': session,
        'result': 'failed on purpose' if is_error else 'done',
    })
    sys.exit(1 if is_error else 0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        sys.stderr.write('fake-claude failed: %s\n' % e)
        sys.stderr.flush()
        sys.exit(1)
'''


REAL_BINS = ('codex', 'claude')
_REAL_POPEN = subprocess.Popen


def _guarded_popen(args, **kwargs):
    prog = ''
    if isinstance(args, (list, tuple)) and args:
        prog = os.path.basename(str(args[0]))
    elif args:
        prog = os.path.basename(str(args).split()[0])
    if prog in REAL_BINS:
        raise AssertionError('tests must not run a real %s binary' % prog)
    return _REAL_POPEN(args, **kwargs)


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError('Condition not met within %.1f s' % timeout)


def delivery_text(request_id, message_id='msg-1'):
    card = cards.card('Lead', 'Coder', body='Please implement the change')
    return cards.envelope(card, request_id, 'lead', 'coder', message_id)


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class FakeClock:
    def __init__(self, now=1_800_000_000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TempDirTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        for target in (
            'agentbrain_handoffs.transports.codex.subprocess.Popen',
            'agentbrain_handoffs.transports.claude_code.subprocess.Popen',
        ):
            patcher = mock.patch(target, new=_guarded_popen)
            patcher.start()
            self.addCleanup(patcher.stop)


class ExperimentalDocTests(unittest.TestCase):
    def test_both_adapters_are_marked_experimental(self):
        self.assertIn('experimental', (CodexTransport.__doc__ or '').lower())
        self.assertIn('experimental', (ClaudeCodeTransport.__doc__ or '').lower())
        import agentbrain_handoffs.transports.codex as codex_mod
        import agentbrain_handoffs.transports.claude_code as claude_mod
        self.assertIn('experimental', (codex_mod.__doc__ or '').lower())
        self.assertIn('experimental', (claude_mod.__doc__ or '').lower())


class CodexCommandTests(unittest.TestCase):
    def test_binary_comes_from_settings_then_constructor_then_env_then_default(self):
        transport = CodexTransport(binary='/from-ctor')
        self.assertEqual(transport.command({'settings': {'codex_bin': '/from-settings'}}), ['/from-settings'])
        self.assertEqual(transport.command({'settings': {'codex_bin': ['wrap', 'codex']}}), ['wrap', 'codex'])
        with mock.patch.dict(os.environ, {'CODEX_BIN': '/from-env'}):
            self.assertEqual(transport.command({'settings': {}}), ['/from-ctor'])
            self.assertEqual(CodexTransport().command({}), ['/from-env'])
        env = {k: v for k, v in os.environ.items() if k != 'CODEX_BIN'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(CodexTransport().command({'settings': {}}), ['codex'])


class ClaudeCommandTests(unittest.TestCase):
    def test_binary_comes_from_settings_then_constructor_then_env_then_default(self):
        transport = ClaudeCodeTransport(binary='/from-ctor')
        self.assertEqual(transport.command({'settings': {'claude_bin': '/from-settings'}}), ['/from-settings'])
        self.assertEqual(transport.command({'settings': {'claude_bin': ['wrap', 'claude']}}), ['wrap', 'claude'])
        with mock.patch.dict(os.environ, {'CLAUDE_BIN': '/from-env'}):
            self.assertEqual(transport.command({'settings': {}}), ['/from-ctor'])
            self.assertEqual(ClaudeCodeTransport().command({}), ['/from-env'])
        env = {k: v for k, v in os.environ.items() if k != 'CLAUDE_BIN'}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(ClaudeCodeTransport().command({'settings': {}}), ['claude'])


class CodexProtocolTests(TempDirTest):
    def setUp(self):
        super().setUp()
        self.fake = self.tmp / 'fake_codex.py'
        self.fake.write_text(FAKE_CODEX)
        self.state_path = self.tmp / 'codex-state.json'
        self.write_state({
            'thread': {'id': 'thread-1', 'status': {'type': 'idle'}},
            'turns': [],
            'next_turn_id': 'codex-turn-99',
            'pids': [],
            'initialize_count': 0,
            'requests': [],
            'errors': {},
            'initialized': False,
            'start_sets_active': True,
            'start_status': 'inProgress',
        })
        os.environ['FAKE_CODEX_STATE'] = str(self.state_path)
        self.addCleanup(os.environ.pop, 'FAKE_CODEX_STATE', None)
        self.binary = [sys.executable, '-u', str(self.fake)]
        self.transport = CodexTransport(binary=self.binary, timeout=5.0, idle_seconds=600)
        self.addCleanup(self.transport.close)

    def write_state(self, data):
        tmp = self.state_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(data))
        tmp.replace(self.state_path)

    def read_state(self):
        with open(self.state_path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return json.load(f)

    def update_state(self, **fields):
        with open(self.state_path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            data = json.load(f)
            data.update(fields)
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        return data

    def agent(self, endpoint='thread-1', **settings):
        return {'id': 'coder', 'name': 'Coder', 'provider': 'codex', 'endpoint': endpoint,
                'cwd': str(self.tmp), 'settings': settings}

    def start(self, request_id='req-1', text=None, agent=None):
        agent = agent or self.agent()
        text = text if text is not None else delivery_text(request_id)
        with self.transport.prepare(agent) as prepared:
            return self.transport.start(prepared, text, request_id)

    def methods(self):
        return [r['method'] for r in self.read_state().get('requests') or []]

    def test_turns_run_as_the_recipient_and_need_a_long_running_host(self):
        self.assertTrue(CodexTransport.needs_host)
        store = Store(self.tmp / 'h.sqlite3')
        transport = CodexTransport(binary=self.binary, timeout=5.0, idle_seconds=600, store=store)
        self.addCleanup(transport.close)
        with mock.patch.dict(os.environ, {'HANDOFFS_AGENT': 'lead', 'HANDOFFS_DB': '/elsewhere.sqlite3'}):
            transport.activity(self.agent())
        state = wait_for(lambda: self.read_state() if self.read_state().get('env') else None)
        self.assertEqual(state['env'], {'HANDOFFS_AGENT': 'coder', 'HANDOFFS_DB': str(store.path.absolute())})

    def test_handshake_initialize_then_initialized_and_app_server_argv(self):
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'finished')
        state = wait_for(lambda: self.read_state() if self.read_state().get('initialized') else None)
        self.assertEqual(state['initialize_count'], 1)
        self.assertEqual(state['clientInfo']['name'], 'agentbrain-handoffs')
        self.assertIn('app-server', state['argv'])
        self.assertEqual(self.methods()[:2], ['initialize', 'initialized'])

    def test_activity_maps_thread_and_latest_turn(self):
        agent = self.agent()
        self.assertEqual(self.transport.activity(agent), {'turnStatus': 'finished', 'turnId': None})

        self.update_state(thread={'id': 'thread-1', 'status': {'type': 'active'}},
                          turns=[{'id': 't-live', 'status': 'inProgress', 'items': []}])
        seen = self.transport.activity(agent)
        self.assertEqual(seen['turnStatus'], 'open')
        self.assertEqual(seen['turnId'], 't-live')

        for kind, expected in (
            ('completed', 'finished'),
            ('failed', 'failed'),
            ('interrupted', 'stopped'),
            ('inProgress', 'open'),
        ):
            self.update_state(thread={'id': 'thread-1', 'status': {'type': 'idle'}},
                              turns=[{'id': 't-' + kind, 'status': kind, 'items': []}])
            seen = self.transport.activity(agent)
            self.assertEqual(seen['turnStatus'], expected, kind)
            self.assertEqual(seen['turnId'], 't-' + kind)

        self.update_state(thread={'id': 'thread-1', 'status': {'type': 'notLoaded'}}, turns=[])
        self.assertEqual(self.transport.activity(agent), {'turnStatus': 'finished', 'turnId': None})

        self.update_state(errors={'thread/read': {'code': -32000, 'message': 'boom'}})
        self.assertEqual(self.transport.activity(agent)['turnStatus'], 'unknown')

    def test_prepare_resumes_unloaded_thread_and_refuses_a_different_id(self):
        self.update_state(thread={'id': 'thread-1', 'status': {'type': 'notLoaded'}})
        with self.transport.prepare(self.agent()) as prepared:
            self.assertEqual(prepared['endpoint'], 'thread-1')
        self.assertTrue(self.read_state().get('resumed'))

        self.update_state(thread={'id': 'other-thread', 'status': {'type': 'idle'}})
        with self.assertRaises(RuntimeError):
            with self.transport.prepare(self.agent()):
                pass

    def test_prepare_raises_not_accepted_on_active_writer(self):
        self.update_state(errors={'thread/read': {'code': -32000, 'message': 'thread has an active writer'}})
        with self.assertRaises(NotAccepted) as caught:
            with self.transport.prepare(self.agent()):
                pass
        self.assertIn('active writer', str(caught.exception).lower())

        self.update_state(
            errors={'thread/resume': {'code': -32000, 'message': 'already in use'}},
            thread={'id': 'thread-1', 'status': {'type': 'notLoaded'}},
        )
        with self.assertRaises(NotAccepted) as caught:
            with self.transport.prepare(self.agent()):
                pass
        self.assertIn('already', str(caught.exception).lower())

    def test_start_keeps_the_app_server_alive_until_observe_sees_the_turn_finish(self):
        request_id = 'req-keep'
        text = delivery_text(request_id)
        receipt = self.start(request_id, text=text)
        self.assertEqual(receipt['turnId'], 'codex-turn-99')
        self.assertEqual(receipt['clientUserMessageId'], request_id)
        state = self.read_state()
        self.assertEqual(state['initialize_count'], 1)
        pid = state['pids'][-1]
        self.assertTrue(pid_alive(pid))
        last = state['last_start']
        self.assertEqual(last['threadId'], 'thread-1')
        self.assertEqual(last['clientUserMessageId'], request_id)
        self.assertEqual(last['input'], [{'type': 'text', 'text': text}])

        running = self.transport.observe(self.agent(), request_id, receipt)
        self.assertEqual(running['status'], 'RUNNING')
        self.assertEqual(self.read_state()['initialize_count'], 1)
        self.assertTrue(pid_alive(pid))
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'open')

        with open(self.state_path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            data = json.load(f)
            data['turns'][0]['status'] = 'completed'
            data['thread']['status'] = {'type': 'idle'}
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())

        done = self.transport.observe(self.agent(), request_id, receipt)
        self.assertEqual(done['status'], 'RETURNED')
        wait_for(lambda: not pid_alive(pid) or self.read_state().get('alive') is False)

    def test_start_raises_not_accepted_when_the_turn_is_refused_before_running(self):
        self.update_state(start_error={'code': -32000, 'message': 'active writer holds the thread'})
        with self.assertRaises(NotAccepted):
            self.start('req-no')

    def test_observe_pages_history_and_matches_by_turn_id_or_marker(self):
        dummy = [{'id': 'old-%d' % i, 'status': 'completed', 'items': []} for i in range(12)]
        marked = {
            'id': 'hidden-turn',
            'status': 'completed',
            'items': [{'type': 'userMessage', 'content': [{'type': 'text', 'text': delivery_text('req-page')}]}],
        }
        self.update_state(turns=dummy + [marked], thread={'id': 'thread-1', 'status': {'type': 'idle'}})
        self.transport.activity(self.agent())

        by_id = self.transport.observe(self.agent(), 'req-page', {'turnId': 'hidden-turn'})
        self.assertEqual(by_id['status'], 'RETURNED')
        self.assertEqual(by_id['receipt']['observedTurnId'], 'hidden-turn')

        by_marker = self.transport.observe(self.agent(), 'req-page', {})
        self.assertEqual(by_marker['status'], 'RETURNED')
        self.assertEqual(by_marker['receipt']['turnId'], 'hidden-turn')

        listed = [r for r in self.read_state()['requests']
                  if r['method'] == 'thread/turns/list' and r['params'].get('limit') == 10]
        self.assertGreaterEqual(len(listed), 2)
        self.assertEqual(listed[0]['params']['sortDirection'], 'desc')
        self.assertNotIn('cursor', listed[0]['params'])
        self.assertEqual(listed[1]['params']['cursor'], '10')

    def test_observe_maps_failed_and_interrupted_and_reports_exhausted_history(self):
        self.update_state(turns=[{
            'id': 't-fail', 'status': 'failed',
            'error': {'message': 'model exploded'},
            'items': [{'type': 'userMessage', 'content': delivery_text('req-fail')}],
        }])
        self.transport.activity(self.agent())
        failed = self.transport.observe(self.agent(), 'req-fail', {'turnId': 't-fail'})
        self.assertEqual(failed['status'], 'FAILED')
        self.assertIn('exploded', failed['detail'])

        self.update_state(turns=[{
            'id': 't-stop', 'status': 'interrupted',
            'items': [{'type': 'userMessage', 'content': delivery_text('req-stop')}],
        }])
        stopped = self.transport.observe(self.agent(), 'req-stop', {'turnId': 't-stop'})
        self.assertEqual(stopped['status'], 'FAILED')
        self.assertEqual(stopped['receipt']['turnOutcome'], 'interrupted')

        self.update_state(turns=[{
            'id': 't-other', 'status': 'completed',
            'items': [{'type': 'userMessage', 'content': 'chatting about req-never in passing'}],
        }])
        mentioned = self.transport.observe(self.agent(), 'req-never', {})
        self.assertIsNone(mentioned['status'])
        self.assertEqual(mentioned['receipt']['historySearch']['exhausted'], True)
        self.assertEqual(mentioned['receipt']['historySearch']['candidate'], 't-other')

        self.update_state(turns=[{'id': 't-x', 'status': 'completed', 'items': []}])
        absent = self.transport.observe(self.agent(), 'req-absent', {})
        self.assertIsNone(absent['status'])
        self.assertEqual(absent['receipt']['historySearch'],
                         {'exhausted': True, 'candidate': None, 'turnId': None})

        self.update_state(always_cursor=True, turns=[{'id': 't-x', 'status': 'completed', 'items': []}])
        incomplete = self.transport.observe(self.agent(), 'req-absent', {})
        self.assertIsNone(incomplete['status'])
        self.assertFalse(incomplete['receipt']['historySearch']['exhausted'])

    def test_one_process_per_endpoint(self):
        self.transport.activity(self.agent('thread-1'))
        first = list(self.read_state()['pids'])
        self.transport.activity(self.agent('thread-2'))
        pids = self.read_state()['pids']
        self.assertEqual(len(set(pids)), 2)
        self.assertEqual(self.read_state()['initialize_count'], 2)
        self.assertEqual(first, pids[:len(first)])

    def test_engine_delivers_through_the_fake_app_server(self):
        clock = FakeClock()
        store = Store(self.tmp / 'h.sqlite3', clock=clock)
        store.register_agent('lead', 'Lead', provider='demo')
        store.register_agent('coder', 'Coder', provider='codex', endpoint='thread-1', cwd=str(self.tmp),
                             settings={'codex_bin': self.binary})
        engine = Engine(store, Router({'codex': self.transport, 'demo': DemoTransport(clock=clock)}), clock=clock)
        mid = store.send('lead', 'coder', 'Please ship the fix', title='Ship it')

        def accepted():
            engine.tick()
            clock.advance(11)
            return store.handoff(mid)['status'] in ('ACCEPTED', 'RUNNING', 'RETURNED')

        wait_for(accepted)
        with open(self.state_path, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            data = json.load(f)
            if data.get('turns'):
                data['turns'][0]['status'] = 'completed'
            data['thread']['status'] = {'type': 'idle'}
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())

        def returned():
            engine.tick()
            clock.advance(11)
            return store.handoff(mid)['status'] == 'RETURNED'

        wait_for(returned)
        row = store.handoff(mid)
        self.assertEqual(row['attempts'], 1)
        self.assertEqual(row['receipt']['turnId'], 'codex-turn-99')


class ClaudeProtocolTests(TempDirTest):
    def setUp(self):
        super().setUp()
        self.fake = self.tmp / 'fake_claude.py'
        self.fake.write_text(FAKE_CLAUDE)
        self.config = self.tmp / 'claude-config'
        self.project = self.config / 'projects' / 'demo-project'
        self.project.mkdir(parents=True)
        self.session_id = 'sess-abc123'
        self.transcript = self.project / (self.session_id + '.jsonl')
        self.transcript.write_text('')
        os.environ['CLAUDE_CONFIG_DIR'] = str(self.config)
        self.addCleanup(os.environ.pop, 'CLAUDE_CONFIG_DIR', None)
        self.state_path = self.tmp / 'claude-state.json'
        self.state_path.write_text('{}')
        os.environ['FAKE_CLAUDE_STATE'] = str(self.state_path)
        self.addCleanup(os.environ.pop, 'FAKE_CLAUDE_STATE', None)
        os.environ['FAKE_CLAUDE_MODE'] = 'ok'
        self.addCleanup(os.environ.pop, 'FAKE_CLAUDE_MODE', None)
        self.release = self.tmp / 'release'
        os.environ['FAKE_CLAUDE_RELEASE'] = str(self.release)
        self.addCleanup(os.environ.pop, 'FAKE_CLAUDE_RELEASE', None)
        self.binary = [sys.executable, '-u', str(self.fake)]
        self.transport = ClaudeCodeTransport(binary=self.binary, start_timeout=5.0)
        self.addCleanup(self.transport.close)

    def agent(self, endpoint=None, **settings):
        return {'id': 'coder', 'name': 'Coder', 'provider': 'claude-code',
                'endpoint': endpoint or self.session_id, 'cwd': str(self.tmp), 'settings': settings}

    def start(self, request_id='req-1', text=None, agent=None):
        agent = agent or self.agent()
        text = text if text is not None else delivery_text(request_id)
        with self.transport.prepare(agent) as prepared:
            return self.transport.start(prepared, text, request_id)

    def write_transcript(self, entries, mtime=None):
        self.transcript.write_text(''.join(json.dumps(e) + '\n' for e in entries))
        if mtime is not None:
            os.utime(self.transcript, (mtime, mtime))

    def claude_state(self):
        try:
            return json.loads(self.state_path.read_text())
        except ValueError:
            return {}

    def test_start_resumes_with_stream_json_and_returns_after_init(self):
        os.environ['FAKE_CLAUDE_MODE'] = 'run'
        text = delivery_text('req-init')
        began = time.monotonic()
        receipt = self.start('req-init', text=text)
        self.assertLess(time.monotonic() - began, 5)
        self.assertEqual(receipt['turnId'], 'req-init')
        self.assertEqual(receipt['clientUserMessageId'], 'req-init')
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'open')
        self.assertEqual(self.transport.observe(self.agent(), 'req-init', receipt)['status'], 'RUNNING')

        state = wait_for(lambda: self.claude_state() if 'prompt' in self.claude_state() else None)
        self.assertEqual(state['argv'][1:], ['-p', '--resume', self.session_id,
                                            '--output-format', 'stream-json', '--verbose'])
        self.assertEqual(state['prompt'], text)
        self.assertEqual(state['session'], self.session_id)

        self.release.touch()
        seen = wait_for(lambda: (
            self.transport.observe(self.agent(), 'req-init', receipt)
            if self.transport.observe(self.agent(), 'req-init', receipt)['status'] != 'RUNNING'
            else None
        ))
        self.assertEqual(seen['status'], 'RETURNED')
        wait_for(lambda: self.transport.activity(self.agent())['turnStatus'] != 'open')
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'finished')

    def test_nonzero_exit_before_any_output_is_not_accepted(self):
        os.environ['FAKE_CLAUDE_MODE'] = 'refuse'
        with self.assertRaises(NotAccepted) as caught:
            self.start('req-no')
        self.assertIn('exited', str(caught.exception).lower())

    def test_error_result_is_failed(self):
        os.environ['FAKE_CLAUDE_MODE'] = 'error'
        receipt = self.start('req-err')
        seen = wait_for(lambda: (
            self.transport.observe(self.agent(), 'req-err', receipt)
            if self.transport.observe(self.agent(), 'req-err', receipt)['status'] != 'RUNNING'
            else None
        ))
        self.assertEqual(seen['status'], 'FAILED')
        self.assertIn('error', seen['detail'].lower())

    def test_activity_uses_transcript_mtime_when_our_process_is_not_running(self):
        now = time.time()
        os.utime(self.transcript, (now, now))
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'open')
        os.utime(self.transcript, (now - 120, now - 120))
        self.assertEqual(self.transport.activity(self.agent()), {'turnStatus': 'finished', 'turnId': None})

    def test_turns_run_as_the_recipient_and_need_a_long_running_host(self):
        self.assertTrue(ClaudeCodeTransport.needs_host)
        store = Store(self.tmp / 'h.sqlite3')
        self.transport = ClaudeCodeTransport(binary=self.binary, start_timeout=5.0, store=store)
        self.addCleanup(self.transport.close)
        with mock.patch.dict(os.environ, {'HANDOFFS_AGENT': 'lead', 'HANDOFFS_DB': '/elsewhere.sqlite3'}):
            self.start('req-env')
        state = wait_for(lambda: self.claude_state() if 'env' in self.claude_state() else None)
        self.assertEqual(state['env'], {'HANDOFFS_AGENT': 'coder', 'HANDOFFS_DB': str(store.path.absolute())})

    def test_a_long_tool_call_keeps_an_interactive_session_busy(self):
        idle_for = time.time() - 120  # nothing written for two minutes
        prompt = {'type': 'user', 'uuid': 'u1', 'message': {'role': 'user', 'content': 'run the full test suite'}}
        tool_call = {'type': 'assistant', 'message': {'role': 'assistant', 'stop_reason': 'tool_use',
                                                      'content': [{'type': 'tool_use', 'name': 'Bash'}]}}
        tool_result = {'type': 'user', 'message': {'content': [{'type': 'tool_result', 'content': 'ok'}]}}
        answer = {'type': 'assistant', 'message': {'stop_reason': 'end_turn', 'content': [{'type': 'text'}]}}
        local = [{'type': 'user', 'message': {'content': '<command-name>/cost</command-name>'}},
                 {'type': 'user', 'message': {'content': '<local-command-stdout>$0.00</local-command-stdout>'}},
                 {'type': 'system', 'subtype': 'stop_hook_summary'}]
        side = {'type': 'assistant', 'isSidechain': True, 'message': {'stop_reason': 'tool_use'}}
        stopped = {'type': 'user', 'message': {'content': [{'type': 'text', 'text': '[Request interrupted by user]'}]}}
        failed = {'type': 'assistant', 'isApiErrorMessage': True, 'message': {}}
        cases = (
            ('a tool call in flight', [prompt, tool_call], 'open'),
            ('a prompt the model has not answered', [prompt], 'open'),
            ('a tool result the model has not answered', [prompt, tool_call, tool_result], 'open'),
            ('an ended turn', [prompt, tool_call, tool_result, answer], 'finished'),
            ('local commands after an ended turn', [prompt, answer] + local, 'finished'),
            ('a subagent writing after an ended turn', [prompt, answer, side], 'finished'),
            ('an interrupted turn', [prompt, tool_call, stopped], 'stopped'),
            ('an API error', [prompt, failed], 'failed'),
        )
        for name, entries, expected in cases:
            with self.subTest(name):
                self.write_transcript(entries, mtime=idle_for)
                self.assertEqual(self.transport.activity(self.agent()), {'turnStatus': expected, 'turnId': None})
        self.write_transcript([prompt, tool_call], mtime=time.time() - 2 * 3600)  # past the one-hour cap
        self.assertEqual(self.transport.activity(self.agent())['turnStatus'], 'stopped')

    def test_a_busy_session_is_not_resumed_under_its_owner(self):
        os.environ['FAKE_CLAUDE_MODE'] = 'ok'
        clock = FakeClock()
        store = Store(self.tmp / 'h.sqlite3', clock=clock)
        store.register_agent('lead', 'Lead', provider='demo')
        store.register_agent('coder', 'Coder', provider='claude-code', endpoint=self.session_id,
                             cwd=str(self.tmp), settings={'claude_bin': self.binary})
        self.write_transcript([
            {'type': 'user', 'message': {'content': 'run the full test suite'}},
            {'type': 'assistant', 'message': {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use'}]}},
        ], mtime=time.time() - 120)
        engine = Engine(store, Router({'claude-code': self.transport, 'demo': DemoTransport(clock=clock)}), clock=clock)
        mid = store.send('lead', 'coder', 'Please ship the fix')
        engine.tick()
        self.assertEqual(store.handoff(mid)['status'], 'BUSY')
        self.assertNotIn('argv', self.claude_state())  # claude -p --resume never ran

    def test_restart_recovers_from_a_transcript_marker(self):
        text = delivery_text('req-7')
        self.write_transcript([
            {'type': 'user', 'uuid': 'u1',
             'message': {'content': [{'type': 'text', 'text': text}]}},
            {'type': 'assistant', 'message': {'stop_reason': 'end_turn'}},
        ])
        fresh = ClaudeCodeTransport(binary=self.binary, start_timeout=5.0)
        seen = fresh.observe(self.agent(), 'req-7', {})
        self.assertEqual(seen['status'], 'RETURNED')
        self.assertEqual(seen['receipt']['foundIn'], 'transcript')

        self.write_transcript([
            {'type': 'user', 'uuid': 'quoted', 'message': {'content': 'talking about req-7 in passing'}},
        ])
        mentioned = fresh.observe(self.agent(), 'req-7', {})
        self.assertIsNone(mentioned['status'])
        self.assertEqual(mentioned['receipt']['historySearch'],
                         {'exhausted': True, 'candidate': 'quoted', 'turnId': None})

        self.write_transcript([
            {'type': 'user', 'uuid': 'u2',
             'message': {'content': [{'type': 'text', 'text': text}]}},
            {'type': 'assistant', 'message': {'content': [{'type': 'tool_use', 'name': 'Bash'}]}},
            {'type': 'user', 'uuid': 'u3', 'message': {'content': 'a new prompt from the owner'}},
        ])
        interrupted = fresh.observe(self.agent(), 'req-7', {})
        self.assertEqual(interrupted['status'], 'FAILED')
        self.assertEqual(interrupted['receipt']['turnOutcome'], 'interrupted')

        self.write_transcript([
            {'type': 'user', 'uuid': 'u4',
             'message': {'content': [{'type': 'text', 'text': text}]}},
            {'type': 'assistant', 'isApiErrorMessage': True, 'message': {}},
        ])
        errored = fresh.observe(self.agent(), 'req-7', {})
        self.assertEqual(errored['status'], 'FAILED')
        self.assertIn('API error', errored['detail'])

        recent = [
            {'type': 'user', 'uuid': 'u5',
             'message': {'content': [{'type': 'text', 'text': text}]}},
            {'type': 'assistant', 'message': {'content': [{'type': 'tool_use', 'name': 'Bash'}]}},
        ]
        self.write_transcript(recent, mtime=time.time())
        running = fresh.observe(self.agent(), 'req-7', {})
        self.assertEqual(running['status'], 'RUNNING')

        self.write_transcript([
            {'type': 'user', 'uuid': 'u6', 'message': {'content': 'unrelated'}},
        ])
        absent = fresh.observe(self.agent(), 'req-missing', {})
        self.assertIsNone(absent['status'])
        self.assertEqual(absent['receipt']['historySearch'],
                         {'exhausted': True, 'candidate': None, 'turnId': None})

    def test_prepare_requires_a_real_session_transcript(self):
        with self.assertRaises(ValueError):
            with self.transport.prepare(self.agent(endpoint='no-such-session')):
                pass

    def test_engine_delivers_through_the_fake_headless_cli(self):
        os.environ['FAKE_CLAUDE_MODE'] = 'ok'
        clock = FakeClock()
        store = Store(self.tmp / 'h.sqlite3', clock=clock)
        store.register_agent('lead', 'Lead', provider='demo')
        store.register_agent('coder', 'Coder', provider='claude-code', endpoint=self.session_id,
                             cwd=str(self.tmp), settings={'claude_bin': self.binary})
        engine = Engine(store, Router({'claude-code': self.transport, 'demo': DemoTransport(clock=clock)}),
                        clock=clock)
        mid = store.send('lead', 'coder', 'Please ship the fix', title='Ship it')
        os.utime(self.transcript, (time.time() - 120, time.time() - 120))

        def returned():
            engine.tick()
            clock.advance(11)
            return store.handoff(mid)['status'] == 'RETURNED'

        wait_for(returned)
        row = store.handoff(mid)
        self.assertEqual(row['attempts'], 1)
        self.assertEqual(row['receipt']['turnId'], row['request_id'])


if __name__ == '__main__':
    unittest.main()
