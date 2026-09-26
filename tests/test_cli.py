"""Tests for the command line, the demo feeder and the outage guard.

Everything runs against temporary databases. Nothing here contacts the network or
starts a real agent: the engine gets a scripted transport, the web page and the
status page are replaced by small fakes.
"""
from __future__ import annotations

import errno
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import types
import unittest
import urllib.request
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from agentbrain_handoffs import __version__, cli, outage
from agentbrain_handoffs.engine import Engine
from agentbrain_handoffs.store import Store
from agentbrain_handoffs.transport import Router, Transport

SRC = str(Path(__file__).resolve().parents[1] / 'src')


def run_cli(*argv, env=None, stdin=''):
    """Run `handoffs ARGV` in-process with a clean environment. Returns (code, stdout, stderr)."""
    clean = {k: v for k, v in os.environ.items() if k not in ('HANDOFFS_DB', 'HANDOFFS_AGENT')}
    clean.update(env or {})
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, clean, clear=True), redirect_stdout(out), redirect_stderr(err), \
            mock.patch.object(sys, 'stdin', io.StringIO(stdin)):
        code = cli.main([str(a) for a in argv])
    return code, out.getvalue(), err.getvalue()


class Clock:
    def __init__(self, at=1_800_000_000.0):
        self.at = at

    def __call__(self):
        return self.at


class FakeTransport(Transport):
    """Always idle, accepts every turn with an exact receipt, and records what it sent."""
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


class CliCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = os.path.join(self.tmp.name, 'handoffs.sqlite3')

    def h(self, *argv, **kwargs):
        return run_cli('--db', self.db, *argv, **kwargs)

    def ok(self, *argv, **kwargs):
        code, out, err = self.h(*argv, **kwargs)
        self.assertEqual(code, 0, 'handoffs ' + ' '.join(map(str, argv)) + ' failed: ' + err)
        return out

    def json(self, *argv, **kwargs):
        return json.loads(self.ok(*argv, '--json', **kwargs))

    def team(self):
        self.ok('init')
        self.ok('agent', 'add', 'planner', '--provider', 'demo', '--name', 'Planner')
        self.ok('agent', 'add', 'engineer', '--provider', 'demo', '--name', 'Engineer')
        return Store(self.db)


class DatabaseLocationTests(CliCase):
    def test_flag_then_environment_then_working_directory(self):
        cwd = os.getcwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.tmp.name)
        self.assertEqual(run_cli('init')[0], 0)
        self.assertTrue(Path(self.tmp.name, '.handoffs', 'handoffs.sqlite3').exists())
        env_db = os.path.join(self.tmp.name, 'from-env.sqlite3')
        flag_db = os.path.join(self.tmp.name, 'from-flag.sqlite3')
        self.assertEqual(run_cli('init', env={'HANDOFFS_DB': env_db})[0], 0)
        self.assertTrue(Path(env_db).exists())
        self.assertEqual(run_cli('--db', flag_db, 'init', env={'HANDOFFS_DB': env_db})[0], 0)
        self.assertTrue(Path(flag_db).exists())

    def test_db_option_is_accepted_after_the_command_too(self):
        self.ok('init')
        code, out, _ = run_cli('status', '--db', self.db)
        self.assertEqual(code, 0)
        self.assertIn(self.db, out)

    def test_commands_other_than_init_never_create_a_database(self):
        code, _, err = self.h('status')
        self.assertEqual(code, 1)
        self.assertIn('handoffs init', err)
        self.assertFalse(Path(self.db).exists())

    def test_init_is_idempotent(self):
        self.assertTrue(self.json('init')['created'])
        self.assertFalse(self.json('init')['created'])


class AgentCommandTests(CliCase):
    def test_add_list_and_remove(self):
        self.ok('init')
        self.ok('agent', 'add', 'planner', '--provider', 'demo', '--name', 'Planner')
        agents = self.json('agent', 'list')
        self.assertEqual([(a['id'], a['name'], a['provider']) for a in agents], [('planner', 'Planner', 'demo')])
        self.assertIn('Removed agent planner', self.ok('agent', 'remove', 'planner'))
        self.assertEqual(self.json('agent', 'list'), [])
        self.assertEqual(self.h('agent', 'remove', 'planner')[0], 1)

    def test_a_new_agent_needs_a_known_provider(self):
        self.ok('init')
        self.assertEqual(self.h('agent', 'add', 'x')[0], 2)
        self.assertEqual(self.h('agent', 'add', 'x', '--provider', 'carrier-pigeon')[0], 2)
        self.assertEqual(self.h('agent')[0], 2)

    def test_settings_are_json_when_possible_and_updates_keep_other_fields(self):
        self.ok('init')
        self.ok('agent', 'add', 'bot', '--provider', 'command', '--cwd', self.tmp.name,
                '--set', 'command=["./deliver.sh", "{text_file}"]', '--set', 'timeout=600', '--set', 'label=night shift')
        self.ok('agent', 'add', 'bot', '--name', 'Night bot', '--set', 'timeout=30')
        agent = Store(self.db).agent('bot')
        self.assertEqual(agent['name'], 'Night bot')
        self.assertEqual(agent['provider'], 'command')
        self.assertEqual(agent['cwd'], os.path.abspath(self.tmp.name))
        self.assertEqual(agent['settings'], {'command': ['./deliver.sh', '{text_file}'], 'timeout': 30,
                                             'label': 'night shift'})

    def test_setups_that_could_never_deliver_are_refused(self):
        self.ok('init')
        for argv in (['--provider', 'codex'],
                     ['--provider', 'claude-code', '--endpoint', 'session-1'],
                     ['--provider', 'command'],
                     ['--provider', 'command', '--set', 'command=./deliver.sh {text_file}'],
                     ['--provider', 'demo', '--set', 'no-equals-sign']):
            code, _, err = self.h('agent', 'add', 'x', *argv)
            self.assertEqual(code, 2, argv)
            self.assertTrue(err.startswith('handoffs: '), err)
        self.ok('agent', 'add', 'cx', '--provider', 'codex', '--endpoint', 'thread-1')
        self.ok('agent', 'add', 'cc', '--provider', 'claude-code', '--endpoint', 'session-1', '--cwd', self.tmp.name)

    def test_changing_the_endpoint_says_nothing_is_redirected(self):
        self.ok('init')
        self.ok('agent', 'add', 'cx', '--provider', 'codex', '--endpoint', 'thread-1')
        self.assertIn('none are redirected', self.ok('agent', 'add', 'cx', '--endpoint', 'thread-2'))

    def test_provider_choices_match_the_shipped_adapters(self):
        try:
            from agentbrain_handoffs.transports import available
        except ImportError:
            self.skipTest('adapters are not built yet')
        self.assertEqual(set(cli.PROVIDERS), set(available()))


class ConnectionAndConfigTests(CliCase):
    def test_block_stops_sending_and_allow_restores_it(self):
        self.team()
        self.ok('block', 'planner', 'engineer')
        code, _, err = self.h('send', 'planner', 'engineer', 'hello')
        self.assertEqual(code, 1)
        self.assertIn('not allowed', err)
        self.ok('allow', 'planner', 'engineer')
        self.ok('send', 'planner', 'engineer', 'hello')

    def test_connections_need_registered_agents_and_two_different_ones(self):
        self.team()
        self.assertEqual(self.h('allow', 'planner', 'ghost')[0], 1)
        self.assertEqual(self.h('allow', 'planner', 'planner')[0], 2)

    def test_config_shows_and_changes_settings(self):
        self.ok('init')
        self.assertEqual(self.json('config')['connections'], 'open')
        self.assertEqual(self.ok('config', 'connections').strip(), '"open"')
        self.ok('config', 'connections', 'explicit')
        self.ok('config', 'enabled', 'false')
        before = time.time()
        self.ok('config', 'enabledAfter', 'now')
        settings = Store(self.db).settings()
        self.assertEqual(settings['connections'], 'explicit')
        self.assertIs(settings['enabled'], False)
        self.assertGreaterEqual(settings['enabledAfter'], before)

    def test_config_rejects_bad_keys_and_values(self):
        self.ok('init')
        self.assertEqual(self.h('config', 'colour', 'blue')[0], 2)
        self.assertEqual(self.h('config', 'enabled', 'maybe')[0], 2)
        self.assertEqual(self.h('config', 'enabledAfter', 'soon')[0], 2)
        self.assertEqual(self.h('config', 'connections', 'sometimes')[0], 1)
        self.assertEqual(self.h('config', 'outageGuard', '{"preset": "nope"}')[0], 2)
        self.assertEqual(self.h('config', 'outageGuard', '{"url": "ftp://x", "components": ["A"]}')[0], 2)
        self.assertIsNone(Store(self.db).settings()['outageGuard'])

    def test_outage_guard_can_be_turned_on_and_off(self):
        self.ok('init')
        self.ok('config', 'outageGuard', '{"preset": "openai-codex"}')
        self.assertEqual(Store(self.db).settings()['outageGuard'], {'preset': 'openai-codex'})
        self.ok('config', 'outageGuard', 'null')
        self.assertIsNone(Store(self.db).settings()['outageGuard'])


class MessageCommandTests(CliCase):
    def test_send_work_with_a_deadline(self):
        self.team()
        sent = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries', '--due-minutes', '30')
        self.assertEqual(sent['work']['title'], 'Retries')
        self.assertEqual(sent['work']['due_seconds'], 1800)
        self.assertEqual(self.h('send', 'planner', 'engineer', 'x', '--due-minutes', '5')[0], 2)
        self.assertEqual(self.h('send', 'planner', 'engineer', 'x', '--title', 'X', '--due-minutes', '0')[0], 2)
        self.assertEqual(self.h('send', 'planner', 'ghost', 'x')[0], 1)

    def test_send_reads_stdin_and_a_key_makes_it_idempotent(self):
        self.team()
        first = self.json('send', 'planner', 'engineer', '-', '--key', 'k1', stdin='From stdin\n')
        again = self.json('send', 'planner', 'engineer', '-', '--key', 'k1', stdin='From stdin\n')
        self.assertEqual(first['id'], again['id'])
        self.assertFalse(first['repeat'])
        self.assertTrue(again['repeat'])
        self.assertEqual(Store(self.db).message(first['id'])['body'], 'From stdin\n')
        self.assertEqual(self.h('send', 'planner', 'engineer', 'different', '--key', 'k1')[0], 1)

    def test_inbox_lists_unread_messages(self):
        self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        inbox = self.json('inbox', 'engineer')
        self.assertEqual([m['id'] for m in inbox], [mid])
        self.assertEqual(inbox[0]['senderName'], 'Planner')
        self.assertEqual(inbox[0]['work']['title'], 'Retries')
        text = self.ok('inbox', 'engineer')
        self.assertIn('1 unread message for Engineer', text)
        self.assertIn(mid, text)
        self.assertEqual(self.h('inbox', 'ghost')[0], 1)

    def test_read_needs_an_identity_and_only_the_recipient_marks_read(self):
        store = self.team()
        self.ok('agent', 'add', 'outsider', '--provider', 'demo')
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        self.assertEqual(self.h('read', mid)[0], 2)
        self.assertEqual(self.h('read', mid, '--as', 'outsider')[0], 1)
        self.assertIn('Add retries', self.ok('read', mid, '--as', 'planner'))
        self.assertIsNone(store.message(mid)['read_at'])
        self.assertIn('handoffs return ' + mid, self.ok('read', mid[:8], env={'HANDOFFS_AGENT': 'engineer'}))
        self.assertIsNotNone(store.message(mid)['read_at'])
        self.assertEqual(self.json('inbox', 'engineer'), [])
        self.assertEqual(len(self.json('inbox', 'engineer', '--all')), 1)

    def test_ids_can_be_shortened_to_a_unique_prefix(self):
        store = self.team()
        with store.connect() as db:
            for mid in ('abcdef01-0000', 'abcdef02-0000'):
                db.execute("INSERT INTO messages(id,sender,recipient,body,intent,created) VALUES(?,?,?,?,?,?)",
                           (mid, 'planner', 'engineer', 'hi', 'handoff', time.time()))
        code, _, err = self.h('read', 'abcdef0', '--as', 'engineer')
        self.assertEqual(code, 1)
        self.assertIn('Several messages', err)
        self.assertIn('hi', self.ok('read', 'abcdef01', '--as', 'engineer'))
        self.assertEqual(self.h('read', 'abc', '--as', 'engineer')[0], 1)

    def test_only_the_assignee_returns_and_only_the_sender_closes(self):
        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        self.assertEqual(self.h('return', mid, '--as', 'planner', '--summary', 'done')[0], 1)
        self.ok('accept', mid, '--as', 'engineer')
        returned = self.json('return', mid, '--as', 'engineer', '--summary', 'Added retries', '--evidence', 'tests pass')
        self.assertEqual(returned['work']['result']['summary'], 'Added retries')
        self.assertEqual(store.message(returned['returnMessage'])['recipient'], 'planner')
        self.assertEqual(self.h('return', mid, '--as', 'engineer', '--summary', 'again')[0], 1)
        self.assertIn('handoffs close ' + mid, self.ok('read', returned['returnMessage'], '--as', 'planner'))
        self.assertEqual(self.h('close', mid, '--as', 'engineer', 'accepted')[0], 1)
        self.assertEqual(self.h('close', mid, '--as', 'planner', 'maybe')[0], 2)
        closed = self.json('close', mid, '--as', 'planner', 'revision', '--note', 'Cap the backoff')
        self.assertEqual(closed['closure']['outcome'], 'revision')
        self.assertEqual(closed['closure']['note'], 'Cap the backoff')

    def test_blocked_returns_are_reported_as_blockers(self):
        self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        text = self.ok('return', mid, '--as', 'engineer', '--summary', 'Need the API key scope', '--blocked')
        self.assertIn('Reported a blocker', text)
        self.assertEqual(Store(self.db).work(mid)['result']['disposition'], 'BLOCKED')


class StatusTests(CliCase):
    def test_status_reports_agents_deliveries_work_and_engine(self):
        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries', '--due-minutes', '30')['id']
        report = self.json('status')
        self.assertEqual({a['id']: a['openWork'] for a in report['agents']}, {'planner': 0, 'engineer': 1})
        self.assertEqual(report['notYetQueued'], 1)
        self.assertFalse(report['engine']['running'])
        self.assertEqual([w['message_id'] for w in report['work']], [mid])
        self.assertFalse(report['work'][0]['overdue'])
        Engine(store, FakeTransport()).enroll(store.message(mid))
        store.update(mid, 'HELD', 'The connection from sender to recipient is not allowed.')
        report = self.json('status')
        self.assertEqual(report['counts'], {'HELD': 1})
        self.assertEqual(report['handoffs'][0]['title'], 'Retries')
        self.assertTrue(report['handoffs'][0]['needsAttention'])
        text = self.ok('status')
        self.assertIn('Engine: never ran', text)
        self.assertIn('Needs attention', text)
        self.assertIn('due in', text)

    def test_status_sees_a_running_engine(self):
        store = self.team()
        health = store.path.with_name(store.path.name + '.health.json')
        health.write_text(json.dumps({'at': time.time(), 'ok': True}))
        self.assertTrue(self.json('status')['engine']['running'])
        self.assertIn('Engine: running', self.ok('status'))


class EngineCommandTests(CliCase):
    def fake_router(self, transport):
        return mock.patch.object(cli, 'make_router', lambda store, speed=1.0: Router({'demo': transport}))

    def test_tick_delivers_one_turn_to_the_exact_recipient(self):
        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        transport = FakeTransport()
        with self.fake_router(transport):
            report = self.json('tick')
        self.assertEqual(report['enrolled'], 1)
        self.assertEqual(store.handoff(mid)['status'], 'ACCEPTED')
        self.assertEqual([agent for agent, _, _ in transport.sent], ['engineer'])
        with self.fake_router(transport):
            self.assertIn('Checked', self.ok('tick'))
        self.assertEqual(len(transport.sent), 1)

    def test_tick_with_the_shipped_demo_adapter(self):
        try:
            import agentbrain_handoffs.transports  # noqa: F401
        except ImportError:
            self.skipTest('adapters are not built yet')
        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        self.ok('tick')
        self.assertEqual(store.handoff(mid)['status'], 'ACCEPTED')

    def test_engine_uses_the_saved_outage_guard(self):
        store = self.team()
        store.configure(outageGuard={'url': 'http://127.0.0.1:9/summary.json', 'components': ['API']})
        with self.fake_router(FakeTransport()):
            engine = cli.build_engine(store)
        self.assertIsInstance(engine.outage, outage.StatusPageGuard)
        self.assertEqual(engine.outage.components, ('API',))

    def test_run_stops_cleanly_on_ctrl_c(self):
        self.team()
        with self.fake_router(FakeTransport()), \
                mock.patch.object(Engine, 'run_forever', side_effect=KeyboardInterrupt):
            self.assertIn('Stopped.', self.ok('run', '--interval', '0.5'))

    def test_tick_leaves_turns_that_need_a_host_for_a_long_running_engine(self):
        class HostedTransport(FakeTransport):
            needs_host = True
            closed = False

            def close(self):
                HostedTransport.closed = True

        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        transport = HostedTransport()
        with self.fake_router(transport):
            self.json('tick')
        row = store.handoff(mid)
        self.assertEqual((row['status'], row['attempts']), ('WAITING', 0))
        self.assertIn('handoffs run', row['detail'])
        self.assertEqual(transport.sent, [])
        self.assertTrue(HostedTransport.closed)  # tick closes what its adapters opened

    def test_release_frees_a_stuck_delivery(self):
        store = self.team()
        mid = self.json('send', 'planner', 'engineer', 'Add retries', '--title', 'Retries')['id']
        with self.fake_router(FakeTransport()):
            self.ok('tick')
        store.update(mid, 'UNCERTAIN', 'Delivery result unknown (timed out).')
        self.assertIn('handoffs release ID', self.ok('status'))
        out = self.ok('release', mid[:8], '--note', 'checked by hand')
        self.assertIn('Released ' + mid, out)
        self.assertIn('it was UNCERTAIN', out)
        row = store.handoff(mid)
        self.assertEqual(row['status'], 'CANCELLED')
        self.assertIn('checked by hand', row['detail'])
        code, _, err = self.h('release', mid)
        self.assertEqual(code, 1)
        self.assertIn('already ended', err)

    def test_mcp_refuses_an_unknown_agent(self):
        self.team()
        code, out, err = self.h('mcp', '--agent', 'ghost')
        self.assertEqual((code, out), (1, ''))
        self.assertIn('Unknown agent ghost', err)


AGENT_WITH_OUTPUT = r'''
import sys, time
print("working", flush=True)
time.sleep(1.0)
for i in range(200):
    print("progress line", i, flush=True)  # written after the tick process has exited
sys.exit(0)
'''


class CommandAgentTickTests(CliCase):
    """The documented one-shot flow, with real processes: `tick` starts a command and exits."""

    def handoffs(self, *argv):
        env = {k: v for k, v in os.environ.items() if k not in ('HANDOFFS_DB', 'HANDOFFS_AGENT')}
        env['PYTHONPATH'] = SRC
        done = subprocess.run([sys.executable, '-m', 'agentbrain_handoffs', '--db', self.db, *argv],
                              env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        return done.stdout

    def test_a_command_started_by_tick_is_seen_to_finish_by_a_later_tick(self):
        self.ok('init')
        script = os.path.join(self.tmp.name, 'agent.py')
        Path(script).write_text(AGENT_WITH_OUTPUT)
        self.ok('agent', 'add', 'planner', '--provider', 'demo')
        self.ok('agent', 'add', 'builder', '--provider', 'command', '--cwd', self.tmp.name,
                '--set', 'command=' + json.dumps([sys.executable, script]))
        first = self.json('send', 'planner', 'builder', 'First job', '--title', 'First')['id']
        self.handoffs('tick')
        store = Store(self.db)
        self.assertEqual(store.handoff(first)['status'], 'ACCEPTED')
        # The tick process is gone; the command keeps running and printing on its own.
        runs = Path(self.db + '.command-runs')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            records = [json.loads(p.read_text()) for p in runs.glob('*.json')]
            if records and records[0].get('status') not in ('starting', 'open'):
                break
            time.sleep(0.05)
        self.assertEqual(records[0]['status'], 'finished')
        self.assertEqual(records[0]['exitCode'], 0)
        second = self.json('send', 'planner', 'builder', 'Second job', '--title', 'Second')['id']
        with store.connect() as db:
            db.execute('UPDATE handoffs SET next_check=0')  # skip the real-time wait for the next check
        self.handoffs('tick')
        row = store.handoff(first)
        self.assertEqual(row['status'], 'RETURNED')
        self.assertIn('progress line 199', row['receipt']['outputTail'])
        self.assertIn(store.handoff(second)['status'], ('ACCEPTED', 'RUNNING', 'RETURNED'))


class FakeWeb:
    """Stands in for agentbrain_handoffs.web: records the serve() call and returns."""

    def __init__(self, error=None):
        self.calls = []
        self.error = error
        self.module = types.ModuleType('agentbrain_handoffs.web')
        self.module.serve = self.serve

    def serve(self, store, engine=None, host='127.0.0.1', port=8765, public_demo=False):
        self.calls.append({'store': store, 'engine': engine, 'host': host, 'port': port, 'public_demo': public_demo,
                           'agents': [a['name'] for a in store.agents()], 'work': store.work_list()})
        if self.error:
            raise self.error

    def installed(self):
        return mock.patch.dict(sys.modules, {'agentbrain_handoffs.web': self.module})


class ServeAndDemoTests(CliCase):
    def setUp(self):
        super().setUp()
        self.router = mock.patch.object(cli, 'make_router', lambda store, speed=1.0: Router({'demo': FakeTransport()}))
        self.router.start()
        self.addCleanup(self.router.stop)

    def test_serve_passes_the_engine_and_options_to_the_page(self):
        self.team()
        web = FakeWeb()
        with web.installed():
            out = self.ok('serve', '--port', '9876')
        self.assertIn('http://127.0.0.1:9876/', out)
        call = web.calls[0]
        self.assertIsInstance(call['engine'], Engine)
        self.assertEqual((call['host'], call['port'], call['public_demo']), ('127.0.0.1', 9876, False))

    def test_serve_refuses_public_demo_on_a_real_database(self):
        self.team()
        web = FakeWeb()
        with web.installed():
            code, out, err = self.h('serve', '--host', '0.0.0.0', '--public-demo')
        self.assertEqual(code, 2)
        self.assertIn('only for "handoffs demo"', err)
        self.assertEqual(web.calls, [])
        self.assertNotIn('--public-demo', self.ok('serve', '--help'))

    def test_a_taken_port_is_a_plain_error(self):
        self.team()
        web = FakeWeb(OSError(errno.EADDRINUSE, 'Address already in use'))
        with web.installed():
            code, _, err = self.h('serve', '--port', '9876')
        self.assertEqual(code, 1)
        self.assertIn('--port', err)

    def test_demo_seeds_four_agents_and_cleans_up(self):
        web = FakeWeb()
        with web.installed():
            code, out, _ = run_cli('demo', '--port', '9877', '--speed', '4')
        self.assertEqual(code, 0)
        self.assertIn('http://127.0.0.1:9877/', out)
        call = web.calls[0]
        self.assertEqual(call['agents'], ['Planner', 'Engineer', 'Reviewer', 'Writer'])
        self.assertEqual(len(call['work']), 3)
        self.assertTrue(all(w['due_seconds'] for w in call['work']))
        self.assertFalse(call['public_demo'])
        self.assertFalse(call['store'].path.parent.exists(), 'the temporary demo folder is removed')
        self.assertEqual(run_cli('demo', '--speed', '0')[0], 2)

    def test_demo_passes_public_demo_and_host_to_the_page(self):
        web = FakeWeb()
        with web.installed():
            code, out, _ = run_cli('demo', '--host', '0.0.0.0', '--port', '9878', '--public-demo')
        self.assertEqual(code, 0)
        self.assertIn('http://127.0.0.1:9878/', out)
        self.assertIn('Public demo mode', out)
        call = web.calls[0]
        self.assertEqual((call['host'], call['port'], call['public_demo']), ('0.0.0.0', 9878, True))


class DemoFeederTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.clock = Clock()
        self.store = Store(os.path.join(tmp.name, 'demo.sqlite3'), clock=self.clock)
        cli.seed_demo(self.store)

    def feeder(self, **options):
        return cli.DemoFeeder(self.store, clock=self.clock, **options)

    def open_titles(self):
        return sorted(w['title'] for w in self.store.work_list() if not w['returned'] and not w['closed'])

    def test_seed_sends_three_assignments_and_a_note(self):
        self.assertEqual([a['id'] for a in self.store.agents()], ['planner', 'engineer', 'reviewer', 'writer'])
        self.assertEqual(self.open_titles(), sorted(w[2] for w in cli.DEMO_WORK[:3]))
        self.assertEqual(len(self.store.inbox('planner')), 1)

    def test_returned_work_is_closed_once_the_sender_saw_the_return(self):
        feeder = self.feeder()
        mid = self.store.work_list()[-1]['message_id']
        sender = self.store.message(mid)
        rid = self.store.return_work(mid, sender['recipient'], 'Done and tested.')
        self.assertEqual(feeder.close_returned(), 0)
        Engine(self.store, FakeTransport()).enroll(self.store.message(rid))
        self.store.update(rid, 'RETURNED', 'The sender reviewed it.')
        self.assertEqual(feeder.close_returned(), 1)
        self.assertIn(self.store.work(mid)['closure']['outcome'], ('accepted', 'revision'))

    def test_unseen_returns_close_after_a_minute_and_blockers_close_as_blocked(self):
        feeder = self.feeder()
        mid = self.store.work_list()[-1]['message_id']
        self.store.return_work(mid, self.store.message(mid)['recipient'], 'Need access', blocked=True)
        self.clock.at += 61
        self.assertEqual(feeder.close_returned(), 1)
        self.assertEqual(self.store.work(mid)['closure']['outcome'], 'blocked')

    def test_stuck_work_is_closed_after_25_minutes(self):
        feeder = self.feeder()
        self.clock.at += 25 * 60 + 1
        self.assertEqual(feeder.close_returned(), 3)
        self.assertEqual(self.open_titles(), [])

    def test_top_up_waits_for_the_gap_and_respects_the_open_limit(self):
        self.assertEqual(self.feeder(max_open=3, gap=12).top_up(), 0)
        feeder = self.feeder(max_open=5, gap=12)
        self.clock.at += 11
        self.assertEqual(feeder.top_up(), 0)
        self.clock.at += 2
        self.assertEqual(feeder.top_up(), 1)
        self.assertIn(cli.DEMO_WORK[3][2], self.open_titles())
        self.assertEqual(feeder.top_up(), 0)

    def test_top_up_never_repeats_an_open_title(self):
        sender, recipient, title, body, due = cli.DEMO_WORK[3]
        self.store.send(sender, recipient, body, title=title, due_seconds=due * 60)
        feeder = self.feeder(max_open=10, gap=0)
        self.assertEqual(feeder.top_up(), 1)
        self.assertIn(cli.DEMO_WORK[4][2], self.open_titles())
        self.assertEqual(len(self.open_titles()), len(set(self.open_titles())))

    def test_prune_removes_old_finished_rows_and_keeps_open_work(self):
        engine = Engine(self.store, FakeTransport())
        engine.enroll_new()
        work = {w['title']: w['message_id'] for w in self.store.work_list()}
        done, still_open = work[cli.DEMO_WORK[0][2]], work[cli.DEMO_WORK[1][2]]
        rid = self.store.return_work(done, 'engineer', 'Done.')
        engine.enroll_new()
        self.store.close_work(done, 'planner', 'accepted')
        for hid in (done, rid, still_open):
            self.store.update(hid, 'RETURNED', 'Turn finished.')
        self.clock.at += 7201
        feeder = self.feeder()
        self.assertEqual(feeder.prune(), 2)
        self.assertIsNone(self.store.handoff(done))
        self.assertIsNone(self.store.handoff(rid))
        self.assertIsNotNone(self.store.handoff(still_open))
        self.assertIsNotNone(self.store.work(still_open))


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


class EntryPointTests(unittest.TestCase):
    def test_python_dash_m_runs_the_cli(self):
        env = {**os.environ, 'PYTHONPATH': SRC}
        done = subprocess.run([sys.executable, '-m', 'agentbrain_handoffs', '--version'], capture_output=True,
                              text=True, env=env, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(done.stdout.strip(), 'handoffs ' + __version__)

    def test_usage_errors_exit_with_2(self):
        self.assertEqual(run_cli()[0], 2)
        self.assertEqual(run_cli('fly')[0], 2)
        self.assertEqual(run_cli('send', 'only-one-arg')[0], 2)
        self.assertEqual(run_cli('--help')[0], 0)

    def test_every_subcommand_help_runs(self):
        for argv in (['--help'], ['--version'],
                     ['init', '--help'], ['agent', '--help'], ['agent', 'add', '--help'],
                     ['agent', 'list', '--help'], ['agent', 'remove', '--help'],
                     ['allow', '--help'], ['block', '--help'], ['config', '--help'],
                     ['send', '--help'], ['inbox', '--help'], ['read', '--help'],
                     ['accept', '--help'], ['return', '--help'], ['close', '--help'],
                     ['status', '--help'], ['tick', '--help'], ['run', '--help'],
                     ['serve', '--help'], ['mcp', '--help'], ['demo', '--help']):
            code, _, err = run_cli(*argv)
            self.assertEqual(code, 0, 'handoffs ' + ' '.join(argv) + ' failed: ' + err)

    def test_help_does_not_import_web_mcp_or_demo(self):
        """web, mcp and the demo adapter load only inside the commands that need them."""
        env = {**os.environ, 'PYTHONPATH': SRC}
        script = ('import sys\n'
                  'from agentbrain_handoffs.cli import main\n'
                  'main(["--help"])\n'
                  'loaded = set(sys.modules)\n'
                  'for name in ("agentbrain_handoffs.web", "agentbrain_handoffs.mcp",\n'
                  '             "agentbrain_handoffs.transports.demo"):\n'
                  '    assert name not in loaded, name\n')
        done = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                              env=env, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr + done.stdout)


@unittest.skipUnless(os.environ.get('HANDOFFS_E2E'), 'set HANDOFFS_E2E=1 to run the live demo check (about 30-60 s)')
class LiveDemoTests(unittest.TestCase):
    """The whole loop through a real process: handoffs move to RETURNED and work gets closed."""

    def test_demo_page_shows_work_returned_and_closed(self):
        import socket
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        env = {**os.environ, 'PYTHONPATH': SRC}
        proc = subprocess.Popen([sys.executable, '-m', 'agentbrain_handoffs', 'demo', '--port', str(port), '--speed', '4'],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(proc.wait, 10)
        self.addCleanup(proc.terminate)
        deadline, seen = time.time() + 90, {}
        while time.time() < deadline:
            try:
                with urllib.request.urlopen('http://127.0.0.1:' + str(port) + '/api/status', timeout=5) as r:
                    status = json.loads(r.read())
            except OSError:
                time.sleep(0.5)
                continue
            seen['returned'] = any(h.get('status') == 'RETURNED' for h in status.get('handoffs', []))
            seen['closed'] = any(w.get('closed') for w in status.get('work', []))
            if seen['returned'] and seen['closed']:
                return
            time.sleep(1)
        self.fail('Within 90 s the demo did not show a RETURNED handoff and closed work: ' + json.dumps(seen))


if __name__ == '__main__':
    unittest.main()
