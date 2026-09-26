"""MCP server tests: the protocol, every tool, the fixed identity, and a real stdio pipe."""
import io
import json
import os
import select
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from agentbrain_handoffs import mcp
from agentbrain_handoffs.engine import Engine
from agentbrain_handoffs.mcp import McpServer, serve_mcp
from agentbrain_handoffs.store import Store
from agentbrain_handoffs.transport import Transport

SRC = Path(__file__).resolve().parents[1] / 'src'
TOOLS = ['send_handoff', 'inbox', 'read_message', 'accept_work', 'return_work', 'close_work',
         'handoff_status', 'list_agents']


class Clock:
    def __init__(self, now=1_800_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


class ReadyTransport(Transport):
    """An always-idle agent app that accepts every turn."""
    name = 'ready'

    def activity(self, agent):
        return {'turnStatus': 'finished', 'turnId': None}

    def start(self, prepared, text, request_id):
        return {'turnId': 'turn-' + request_id, 'clientUserMessageId': request_id}

    def observe(self, agent, request_id, receipt):
        return {'status': 'RUNNING', 'detail': 'The turn is running.', 'receipt': {}}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'handoffs.sqlite3'
        self.clock = Clock()
        self.store = Store(self.db, clock=self.clock)
        for agent_id, name in (('planner', 'Planner'), ('engineer', 'Engineer'), ('reviewer', 'Reviewer')):
            self.store.register_agent(agent_id, name=name, provider='ready')
        self.ids = iter(range(1, 10000))
        self.as_planner = self.server('planner')
        self.as_engineer = self.server('engineer')
        self.as_reviewer = self.server('reviewer')

    def server(self, agent_id, protocol='2025-06-18'):
        server = McpServer(self.store, agent_id)
        server.handle({'jsonrpc': '2.0', 'id': 0, 'method': 'initialize',
                       'params': {'protocolVersion': protocol, 'capabilities': {},
                                  'clientInfo': {'name': 'test', 'version': '1'}}})
        return server

    def rpc(self, server, method, params=None):
        message = {'jsonrpc': '2.0', 'id': next(self.ids), 'method': method}
        if params is not None:
            message['params'] = params
        return server.handle(message)

    def call(self, server, tool, **arguments):
        reply = self.rpc(server, 'tools/call', {'name': tool, 'arguments': arguments})
        self.assertIn('result', reply, reply)
        return reply['result']

    def ok(self, server, tool, **arguments):
        result = self.call(server, tool, **arguments)
        self.assertFalse(result['isError'], result['content'][0]['text'])
        return result

    def fails(self, server, tool, **arguments):
        result = self.call(server, tool, **arguments)
        self.assertTrue(result['isError'], 'expected an error from ' + tool)
        self.assertNotIn('Traceback', text(result))
        return text(result)

    def assign(self, title='Fix the login bug', **extra):
        result = self.ok(self.as_planner, 'send_handoff', to='engineer', message='Please fix login.', title=title, **extra)
        return result['structuredContent']['messageId']


def text(result):
    return result['content'][0]['text']


class ProtocolTests(Base):
    def test_initialize_echoes_each_supported_version(self):
        for version in ('2024-11-05', '2025-03-26', '2025-06-18'):
            reply = McpServer(self.store, 'engineer').handle(
                {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': version}})
            self.assertEqual(reply['result']['protocolVersion'], version)

    def test_initialize_offers_latest_for_unknown_or_missing_version(self):
        for params in ({'protocolVersion': '1999-01-01'}, {'protocolVersion': 7}, {}):
            reply = McpServer(self.store, 'engineer').handle(
                {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': params})
            self.assertEqual(reply['result']['protocolVersion'], '2025-06-18')

    def test_initialize_describes_server_and_identity(self):
        result = McpServer(self.store, 'engineer').handle(
            {'jsonrpc': '2.0', 'id': 'a', 'method': 'initialize', 'params': {'protocolVersion': '2025-03-26'}})
        self.assertEqual(result['id'], 'a')
        result = result['result']
        self.assertEqual(result['capabilities'], {'tools': {'listChanged': False}})
        self.assertEqual(result['serverInfo']['name'], 'agentbrain-handoffs')
        self.assertTrue(result['serverInfo']['version'])
        self.assertIn('Engineer (engineer)', result['instructions'])

    def test_notifications_get_no_reply(self):
        server = McpServer(self.store, 'engineer')
        self.assertIsNone(server.handle({'jsonrpc': '2.0', 'method': 'notifications/initialized'}))
        self.assertIsNone(server.handle({'jsonrpc': '2.0', 'method': 'notifications/cancelled',
                                         'params': {'requestId': 3}}))
        self.assertIsNone(server.handle({'jsonrpc': '2.0', 'method': 'no/such/notification'}))
        self.assertIsNone(server.handle({'jsonrpc': '2.0', 'id': 9, 'result': {}}))  # a client reply

    def test_ping(self):
        self.assertEqual(self.rpc(self.as_engineer, 'ping')['result'], {})

    def test_unknown_method(self):
        reply = self.rpc(self.as_engineer, 'resources/list')
        self.assertEqual(reply['error']['code'], -32601)

    def test_parse_and_request_errors(self):
        server = self.as_engineer
        parse = json.loads(server.handle_line('{not json'))
        self.assertEqual((parse['id'], parse['error']['code']), (None, -32700))
        self.assertEqual(json.loads(server.handle_line(b'\xff\xfe\n'))['error']['code'], -32700)
        self.assertEqual(server.handle('text')['error']['code'], -32600)
        self.assertEqual(server.handle({'id': 1, 'method': 'ping'})['error']['code'], -32600)  # no jsonrpc
        self.assertEqual(server.handle({'jsonrpc': '2.0', 'id': {}, 'method': 'ping'})['error']['code'], -32600)
        self.assertEqual(server.handle({'jsonrpc': '2.0', 'id': 1, 'method': 'ping', 'params': []})['error']['code'],
                         -32602)
        self.assertIsNone(server.handle_line('   \n'))

    def test_batch(self):
        replies = self.as_engineer.handle([{'jsonrpc': '2.0', 'id': 1, 'method': 'ping'},
                                           {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
                                           {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'}])
        self.assertEqual([r['id'] for r in replies], [1, 2])
        self.assertEqual(self.as_engineer.handle([])['error']['code'], -32600)

    def test_unknown_tool_is_a_protocol_error(self):
        reply = self.rpc(self.as_engineer, 'tools/call', {'name': 'delete_everything', 'arguments': {}})
        self.assertEqual(reply['error']['code'], -32602)
        self.assertIn('send_handoff', reply['error']['message'])
        self.assertEqual(self.rpc(self.as_engineer, 'tools/call', {})['error']['code'], -32602)

    def test_tools_list(self):
        tools = self.rpc(self.as_engineer, 'tools/list')['result']['tools']
        self.assertEqual([t['name'] for t in tools], TOOLS)
        for tool in tools:
            schema = tool['inputSchema']
            self.assertEqual(schema['type'], 'object')
            self.assertFalse(schema['additionalProperties'])
            self.assertGreater(len(tool['description']), 40)
            self.assertTrue(tool['title'])
            self.assertFalse(tool['annotations']['openWorldHint'])
            for name, prop in schema['properties'].items():
                self.assertTrue(prop.get('description'), tool['name'] + '.' + name)
            # The identity can never be chosen per call.
            self.assertFalse(set(schema['properties']) & {'from', 'sender', 'as', 'agent', 'agent_id'})
        by_name = {t['name']: t for t in tools}
        self.assertEqual(by_name['send_handoff']['inputSchema']['required'], ['to', 'message'])
        self.assertEqual(by_name['close_work']['inputSchema']['properties']['outcome']['enum'],
                         ['accepted', 'revision', 'blocked'])
        self.assertTrue(by_name['inbox']['annotations']['readOnlyHint'])
        self.assertIn('Engineer (engineer)', by_name['send_handoff']['description'])

    def test_older_protocols_get_only_fields_they_know(self):
        old = self.server('engineer', protocol='2024-11-05')
        tool = self.rpc(old, 'tools/list')['result']['tools'][0]
        self.assertNotIn('annotations', tool)
        self.assertNotIn('title', tool)
        result = self.call(old, 'list_agents')
        self.assertNotIn('structuredContent', result)
        middle = self.server('engineer', protocol='2025-03-26')
        tool = self.rpc(middle, 'tools/list')['result']['tools'][0]
        self.assertIn('annotations', tool)
        self.assertNotIn('title', tool)
        self.assertIn('structuredContent', self.call(self.as_engineer, 'list_agents'))


class IdentityTests(Base):
    def test_sender_is_always_the_server_identity(self):
        mid = self.assign()
        m = self.store.message(mid)
        self.assertEqual((m['sender'], m['recipient']), ('planner', 'engineer'))

    def test_identity_arguments_are_refused(self):
        for word in ('from', 'sender', 'as', 'agent_id'):
            error = self.fails(self.as_engineer, 'send_handoff', to='reviewer', message='hi', **{word: 'planner'})
            self.assertIn('always acts as Engineer (engineer)', error)
        self.assertEqual(self.store.inbox('reviewer'), [])

    def test_cannot_return_or_close_as_someone_else(self):
        mid = self.assign()
        self.assertIn('Only the assigned agent', self.fails(self.as_planner, 'return_work', id=mid, summary='done'))
        self.assertIn('Only the assigned agent', self.fails(self.as_planner, 'accept_work', id=mid))
        self.ok(self.as_engineer, 'return_work', id=mid, summary='Fixed it.')
        self.assertIn('Only the agent that sent', self.fails(self.as_engineer, 'close_work', id=mid, outcome='accepted'))
        self.assertIsNone(self.store.work(mid)['closed'])

    def test_other_agents_messages_are_invisible(self):
        mid = self.assign()
        for tool, extra in (('read_message', {}), ('handoff_status', {}), ('accept_work', {}),
                            ('return_work', {'summary': 'x'}), ('close_work', {'outcome': 'accepted'})):
            third_party = self.fails(self.as_reviewer, tool, id=mid, **extra)
            unknown = self.fails(self.as_reviewer, tool, id='no-such-id', **extra)
            self.assertEqual(third_party.replace(mid, 'ID'), unknown.replace('no-such-id', 'ID'))
        self.assertIsNone(self.store.message(mid)['read_at'])

    def test_service_and_malformed_ids_cannot_start_a_server(self):
        for bad in ('service:deadline', 'service:router', '', 'has space', 'x' * 201, None):
            with self.assertRaises(ValueError):
                McpServer(self.store, bad)

    def test_unregistered_identity_explains_itself(self):
        ghost = self.server('ghost')
        error = self.fails(ghost, 'inbox')
        self.assertIn('not registered', error)
        self.assertIn('handoffs agent add ghost', error)
        self.fails(ghost, 'send_handoff', to='engineer', message='hello')
        self.assertEqual(self.store.inbox('engineer'), [])
        self.assertIn('3 agents', text(self.ok(ghost, 'list_agents')))  # still works, to help debugging


class SendTests(Base):
    def test_plain_message(self):
        result = self.ok(self.as_planner, 'send_handoff', to='engineer', message='FYI: the build is green.')
        data = result['structuredContent']
        self.assertFalse(data['work'])
        self.assertIn('Sent to Engineer (engineer).', text(result))
        self.assertIn('Message id: ' + data['messageId'], text(result))
        self.assertIsNone(self.store.work(data['messageId']))

    def test_work_with_deadline(self):
        mid = self.assign(due_minutes=90)
        work = self.store.work(mid)
        self.assertEqual((work['title'], work['due_seconds']), ('Fix the login bug', 5400))

    def test_deadline_needs_title(self):
        error = self.fails(self.as_planner, 'send_handoff', to='engineer', message='m', due_minutes=5)
        self.assertIn('needs a title', error)

    def test_bad_recipients(self):
        self.assertIn('yourself', self.fails(self.as_planner, 'send_handoff', to='planner', message='m'))
        error = self.fails(self.as_planner, 'send_handoff', to='Engineer', message='m')
        self.assertIn('Did you mean "engineer" (Engineer)?', error)
        self.assertIn('list_agents', self.fails(self.as_planner, 'send_handoff', to='nobody', message='m'))
        self.store.set_connection('planner', 'reviewer', allow=False)
        self.assertIn('not allowed', self.fails(self.as_planner, 'send_handoff', to='reviewer', message='m'))
        self.assertEqual(self.store.inbox('reviewer'), [])

    def test_key_makes_send_idempotent_per_sender(self):
        first = self.ok(self.as_planner, 'send_handoff', to='engineer', message='deploy', key='k1')
        again = self.ok(self.as_planner, 'send_handoff', to='engineer', message='deploy', key='k1')
        self.assertEqual(first['structuredContent']['messageId'], again['structuredContent']['messageId'])
        self.assertFalse(first['structuredContent']['alreadySent'])
        self.assertTrue(again['structuredContent']['alreadySent'])
        self.assertIn('nothing was sent twice', text(again))
        self.assertEqual(len(self.store.inbox('engineer')), 1)
        self.assertIn('different message',
                      self.fails(self.as_planner, 'send_handoff', to='engineer', message='other', key='k1'))
        # Keys are scoped to the sender: another agent's identical key is its own.
        self.ok(self.as_reviewer, 'send_handoff', to='engineer', message='review', key='k1')
        self.assertEqual(len(self.store.inbox('engineer')), 2)

    def test_notes_about_engine_and_pause(self):
        quiet = text(self.ok(self.as_planner, 'send_handoff', to='engineer', message='one'))
        self.assertIn('No delivery engine has reported', quiet)
        Engine(self.store, ReadyTransport(), clock=self.clock).tick()
        running = text(self.ok(self.as_planner, 'send_handoff', to='engineer', message='two'))
        self.assertNotIn('No delivery engine', running)
        self.assertIn("starts one new turn in Engineer's session", running)
        self.store.configure(enabled=False)
        paused = self.ok(self.as_planner, 'send_handoff', to='engineer', message='three')
        self.assertIn('Delivery is paused', text(paused))
        self.assertTrue(paused['structuredContent']['deliveryPaused'])


class ArgumentTests(Base):
    def test_missing_and_mistyped_arguments(self):
        self.assertIn('Missing to', self.fails(self.as_planner, 'send_handoff', message='m'))
        self.assertIn('message must be text', self.fails(self.as_planner, 'send_handoff', to='engineer', message=5))
        self.assertIn('cannot be empty', self.fails(self.as_planner, 'send_handoff', to='engineer', message='   '))
        self.assertIn('whole number', self.fails(self.as_planner, 'send_handoff', to='engineer', message='m',
                                                 title='t', due_minutes=True))
        self.assertIn('whole number', self.fails(self.as_planner, 'send_handoff', to='engineer', message='m',
                                                 title='t', due_minutes=2.5))
        self.assertIn('between 1 and 43200', self.fails(self.as_planner, 'send_handoff', to='engineer', message='m',
                                                        title='t', due_minutes=0))
        self.assertIn('true or false', self.fails(self.as_engineer, 'inbox', unread_only='yes'))
        self.assertIn('must be one of', self.fails(self.as_planner, 'close_work', id='x', outcome='done'))
        error = self.fails(self.as_planner, 'send_handoff', to='engineer', message='m', recipient='engineer')
        self.assertIn('Unknown argument: recipient', error)
        self.assertIn('Unknown argument', self.fails(self.as_planner, 'list_agents', verbose=True))
        reply = self.rpc(self.as_planner, 'tools/call', {'name': 'inbox', 'arguments': ['x']})
        self.assertTrue(reply['result']['isError'])
        self.assertEqual(self.store.inbox('engineer'), [])

    def test_whole_float_and_null_are_accepted(self):
        mid = self.ok(self.as_planner, 'send_handoff', to=' engineer ', message='m', title='t', due_minutes=2.0,
                      key=None)['structuredContent']['messageId']
        self.assertEqual(self.store.work(mid)['due_seconds'], 120)

    def test_unexpected_failure_is_a_tool_error_not_a_crash(self):
        with mock.patch.object(Store, 'inbox', side_effect=RuntimeError('disk on fire')), \
                mock.patch.object(mcp.McpServer, '_log'):
            error = self.fails(self.as_engineer, 'inbox')
        self.assertIn('unexpected error: disk on fire', error)
        self.ok(self.as_engineer, 'inbox')


class InboxAndReadTests(Base):
    def test_inbox_lists_unread_then_read_marks(self):
        self.assertIn('No unread messages for Engineer (engineer).', text(self.ok(self.as_engineer, 'inbox')))
        mid = self.assign(due_minutes=60)
        self.clock.now += 5
        self.ok(self.as_reviewer, 'send_handoff', to='engineer', message='Nice work yesterday.\nMore soon.')
        result = self.ok(self.as_engineer, 'inbox')
        body = text(result)
        self.assertIn('2 unread messages for Engineer (engineer), oldest first:', body)
        self.assertIn('1. Work "Fix the login bug", not started yet, due ', body)
        self.assertIn('from Planner (planner)', body)
        self.assertIn('2. Message from Reviewer (reviewer)', body)
        self.assertIn('"Nice work yesterday. More soon."', body)
        items = result['structuredContent']['messages']
        self.assertEqual([m['kind'] for m in items], ['work', 'message'])
        self.assertEqual(items[0]['work']['state'], 'open')

        read = self.ok(self.as_engineer, 'read_message', id=mid)
        self.assertTrue(read['structuredContent']['markedRead'])
        self.assertIn('Please fix login.', text(read))
        self.assertIn('call return_work with id ' + mid, text(read))
        self.assertIsNotNone(self.store.message(mid)['read_at'])

        unread = self.ok(self.as_engineer, 'inbox')['structuredContent']['messages']
        self.assertEqual([m['kind'] for m in unread], ['message'])
        everything = text(self.ok(self.as_engineer, 'inbox', unread_only=False))
        self.assertIn('2 messages for Engineer', everything)
        self.assertIn('(unread)', everything)

    def test_sender_can_read_without_marking(self):
        mid = self.assign()
        result = self.ok(self.as_planner, 'read_message', id=mid)
        self.assertFalse(result['structuredContent']['markedRead'])
        self.assertIsNone(self.store.message(mid)['read_at'])
        self.assertIn('Waiting for Engineer (engineer) to return it.', text(result))

    def test_notices_from_services_show_by_name(self):
        self.store.send('service:deadline', 'planner', 'The deadline passed.', intent='notification')
        body = text(self.ok(self.as_planner, 'inbox'))
        self.assertIn('Notice from Deadline service (service:deadline)', body)

    def test_reading_before_delivery_means_no_extra_turn(self):
        mid = self.assign()
        self.ok(self.as_engineer, 'read_message', id=mid)
        Engine(self.store, ReadyTransport(), clock=self.clock).tick()
        self.assertIsNone(self.store.handoff(mid))
        self.assertIn('read before delivery', text(self.ok(self.as_planner, 'handoff_status', id=mid)))


class WorkLifecycleTests(Base):
    def test_accept_return_close(self):
        mid = self.assign(due_minutes=30)
        accepted = self.ok(self.as_engineer, 'accept_work', id=mid)
        self.assertIn('Accepted "Fix the login bug" from Planner (planner)', text(accepted))
        self.assertIsNotNone(self.store.work(mid)['accepted'])

        returned = self.ok(self.as_engineer, 'return_work', id=mid, summary='Fixed the token refresh.',
                           evidence='tests/test_login.py passes')
        rid = returned['structuredContent']['returnMessageId']
        self.assertIn('Returned "Fix the login bug" to Planner (planner) as done.', text(returned))
        back = self.store.message(rid)
        self.assertEqual((back['sender'], back['recipient']), ('engineer', 'planner'))
        self.assertIn('already returned', self.fails(self.as_engineer, 'return_work', id=mid, summary='again'))
        self.assertIn('nothing left to accept', text(self.ok(self.as_engineer, 'accept_work', id=mid)))

        # The sender sees the result and closes it by the return message id.
        result = self.ok(self.as_planner, 'read_message', id=rid)
        self.assertIn('Result for "Fix the login bug", returned', text(result))
        self.assertIn('Result: Fixed the token refresh.', text(result))
        self.assertIn('Evidence: tests/test_login.py passes', text(result))
        self.assertIn('close_work (id ' + mid, text(result))
        closed = self.ok(self.as_planner, 'close_work', id=rid, outcome='accepted', note='Thanks')
        self.assertEqual(closed['structuredContent']['messageId'], mid)
        self.assertEqual(self.store.work(mid)['closure']['outcome'], 'accepted')
        self.assertIn('Decision: accepted. Note: Thanks', text(self.ok(self.as_engineer, 'read_message', id=mid)))

    def test_blocked_return_and_revision(self):
        mid = self.assign()
        self.ok(self.as_engineer, 'return_work', id=mid, summary='No access to the staging database.', blocked=True)
        self.assertEqual(self.store.work(mid)['result']['disposition'], 'BLOCKED')
        closed = text(self.ok(self.as_planner, 'close_work', id=mid, outcome='revision'))
        self.assertIn('Engineer is not notified by closing', closed)

    def test_close_before_return_is_recorded_honestly(self):
        mid = self.assign()
        self.assertIn('closed before a result was returned',
                      text(self.ok(self.as_planner, 'close_work', id=mid, outcome='blocked')))

    def test_plain_messages_are_not_work(self):
        mid = self.ok(self.as_planner, 'send_handoff', to='engineer', message='hi')['structuredContent']['messageId']
        self.assertIn('not a work assignment', self.fails(self.as_engineer, 'accept_work', id=mid))
        self.assertIn('not a work assignment', self.fails(self.as_engineer, 'return_work', id=mid, summary='s'))

    def test_return_message_is_not_the_work(self):
        mid = self.assign()
        rid = self.ok(self.as_engineer, 'return_work', id=mid, summary='done')['structuredContent']['returnMessageId']
        self.assertIn('returned result, not the work', self.fails(self.as_engineer, 'return_work', id=rid, summary='s'))


class StatusAndAgentsTests(Base):
    def test_status_before_and_after_delivery(self):
        mid = self.assign()
        waiting = self.ok(self.as_planner, 'handoff_status', id=mid)
        self.assertIsNone(waiting['structuredContent']['delivery'])
        self.assertIn('handoffs run', text(waiting))

        Engine(self.store, ReadyTransport(), clock=self.clock).tick()
        status = self.ok(self.as_planner, 'handoff_status', id=mid)
        delivery = status['structuredContent']['delivery']
        self.assertEqual((delivery['state'], delivery['phase'], delivery['attempts']), ('ACCEPTED', 'in flight', 1))
        self.assertEqual([e['state'] for e in delivery['history']], ['WAITING', 'SENDING', 'ACCEPTED'])
        self.assertIn('Delivery: ACCEPTED (in flight).', text(status))
        self.assertIn('to Engineer (engineer)', text(status))
        self.assertIn('from Planner (planner)', text(self.ok(self.as_engineer, 'handoff_status', id=mid)))

    def test_overview(self):
        self.assertIn('No deliveries yet.', text(self.ok(self.as_planner, 'handoff_status')))
        mid = self.assign()
        self.ok(self.as_reviewer, 'send_handoff', to='planner', message='Review the plan', title='Plan review')
        Engine(self.store, ReadyTransport(), clock=self.clock).tick()
        result = self.ok(self.as_planner, 'handoff_status')
        body, data = text(result), result['structuredContent']
        self.assertIn(mid + ' to Engineer (engineer): ACCEPTED (in flight).', body)
        self.assertIn('Work you assigned, still open:', body)
        self.assertIn('Work assigned to you, still open:', body)
        self.assertEqual([w['title'] for w in data['workYouAssigned']], ['Fix the login bug'])
        self.assertEqual([w['title'] for w in data['workAssignedToYou']], ['Plan review'])
        self.assertEqual({d['direction'] for d in data['deliveries']}, {'sent', 'received'})
        # The reviewer sees only its own side.
        mine = self.ok(self.as_reviewer, 'handoff_status')['structuredContent']
        self.assertNotIn(mid, [d['id'] for d in mine['deliveries']])

    def test_overdue_work_is_flagged(self):
        mid = self.assign(due_minutes=10)
        self.clock.now += 11 * 60
        view = self.ok(self.as_planner, 'handoff_status', id=mid)['structuredContent']['work']
        self.assertTrue(view['overdue'])
        self.assertIn('(overdue)', text(self.ok(self.as_engineer, 'inbox')))

    def test_list_agents(self):
        self.store.set_connection('engineer', 'reviewer', allow=False)
        result = self.ok(self.as_engineer, 'list_agents')
        body = text(result)
        self.assertIn('3 agents. You are Engineer (engineer).', body)
        self.assertIn('- engineer: Engineer (ready), this is you', body)
        self.assertIn('- planner: Planner (ready), you can send to it', body)
        self.assertIn('- reviewer: Reviewer (ready), you cannot send to it', body)
        agents = {a['id']: a for a in result['structuredContent']['agents']}
        self.assertEqual({k: (a['you'], a['canSend']) for k, a in agents.items()},
                         {'engineer': (True, False), 'planner': (False, True), 'reviewer': (False, False)})
        self.assertNotIn('settings', agents['planner'])  # adapter settings never leak to other agents


class StreamTests(Base):
    def lines(self, *messages):
        return b''.join((m if isinstance(m, bytes) else json.dumps(m).encode()) + b'\n' for m in messages)

    def test_serve_over_byte_streams(self):
        stdin = io.BytesIO(self.lines(
            {'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2025-03-26'}},
            {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
            b'',
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
             'params': {'name': 'send_handoff', 'arguments': {'to': 'planner', 'message': 'Café ☕ ready'}}},
            b'garbage',
            {'jsonrpc': '2.0', 'id': 3, 'method': 'ping'}))
        stdout = io.BytesIO()
        self.assertEqual(serve_mcp(self.store, 'engineer', stdin=stdin, stdout=stdout), 0)
        replies = [json.loads(line) for line in stdout.getvalue().decode('ascii').splitlines()]
        self.assertEqual([r.get('id') for r in replies], [1, 2, None, 3])
        self.assertEqual(replies[0]['result']['protocolVersion'], '2025-03-26')
        self.assertFalse(replies[1]['result']['isError'])
        self.assertEqual(replies[2]['error']['code'], -32700)
        self.assertEqual(self.store.inbox('planner')[0]['body'], 'Café ☕ ready')

    def test_serve_over_text_streams(self):
        stdin = io.StringIO('{"jsonrpc":"2.0","id":7,"method":"ping"}\n')
        stdout = io.StringIO()
        serve_mcp(self.store, 'engineer', stdin=stdin, stdout=stdout)
        self.assertEqual(json.loads(stdout.getvalue()), {'jsonrpc': '2.0', 'id': 7, 'result': {}})

    def test_stray_prints_cannot_corrupt_stdout(self):
        original = McpServer._tool_list_agents

        def noisy(server, args):
            print('stray debugging output')
            return original(server, args)

        out = io.BytesIO()
        fake_stdout = io.TextIOWrapper(out, encoding='utf-8')
        fake_stdin = io.TextIOWrapper(io.BytesIO(self.lines(
            {'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {'name': 'list_agents'}})), encoding='utf-8')
        err = io.StringIO()
        with mock.patch.object(sys, 'stdin', fake_stdin), mock.patch.object(sys, 'stdout', fake_stdout), \
                mock.patch.object(sys, 'stderr', err), mock.patch.object(McpServer, '_tool_list_agents', noisy):
            serve_mcp(self.store, 'engineer')
            written = out.getvalue()
        self.assertIn('stray debugging output', err.getvalue())
        lines = written.decode('ascii').splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])['id'], 1)


class SubprocessTests(unittest.TestCase):
    """The real thing: `python -m agentbrain_handoffs.mcp` over OS pipes, one line at a time."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'handoffs.sqlite3'
        store = Store(self.db)
        store.register_agent('planner', name='Planner', provider='demo')
        store.register_agent('engineer', name='Engineer', provider='demo')
        self.store = store
        self.next_id = 0

    def start(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(SRC) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        env.pop('HANDOFFS_DB', None)
        proc = subprocess.Popen([sys.executable, '-m', 'agentbrain_handoffs.mcp', *args],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                cwd=self.tmp.name, env=env)
        self.addCleanup(self.stop, proc)
        return proc

    @staticmethod
    def stop(proc):
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream and not stream.closed:
                stream.close()

    def read_line(self, proc, timeout=15):
        ready, _, _ = select.select([proc.stdout], [], [], timeout)
        self.assertTrue(ready, 'the server did not answer within ' + str(timeout) + ' s')
        line = proc.stdout.readline()
        self.assertTrue(line.endswith(b'\n'), line)
        return json.loads(line)

    def rpc(self, proc, method, params=None, notify=False):
        message = {'jsonrpc': '2.0', 'method': method}
        if params is not None:
            message['params'] = params
        if not notify:
            self.next_id += 1
            message['id'] = self.next_id
        proc.stdin.write(json.dumps(message).encode() + b'\n')
        proc.stdin.flush()
        if notify:
            return None
        reply = self.read_line(proc)
        self.assertEqual(reply['id'], self.next_id)
        return reply

    def test_full_session_over_a_pipe(self):
        proc = self.start('--db', str(self.db), '--agent', 'engineer')
        init = self.rpc(proc, 'initialize', {'protocolVersion': '2025-06-18', 'capabilities': {},
                                             'clientInfo': {'name': 'pipe-test', 'version': '1'}})
        self.assertEqual(init['result']['protocolVersion'], '2025-06-18')
        self.rpc(proc, 'notifications/initialized', notify=True)
        tools = self.rpc(proc, 'tools/list')['result']['tools']
        self.assertEqual([t['name'] for t in tools], TOOLS)

        mid = self.store.send('planner', 'engineer', 'Write the release notes ✍', title='Release notes')
        inbox = self.rpc(proc, 'tools/call', {'name': 'inbox', 'arguments': {}})['result']
        self.assertEqual(inbox['structuredContent']['messages'][0]['id'], mid)
        self.assertIn('✍', inbox['content'][0]['text'])

        sent = self.rpc(proc, 'tools/call', {'name': 'return_work',
                                             'arguments': {'id': mid, 'summary': 'Notes are in CHANGELOG.md'}})
        self.assertFalse(sent['result']['isError'], sent)
        self.assertEqual(self.store.work(mid)['result']['summary'], 'Notes are in CHANGELOG.md')

        refused = self.rpc(proc, 'tools/call', {'name': 'close_work', 'arguments': {'id': mid, 'outcome': 'accepted'}})
        self.assertTrue(refused['result']['isError'])
        self.assertIsNone(self.store.work(mid)['closed'])

        proc.stdin.write(b'{"broken\n')
        proc.stdin.flush()
        self.assertEqual(self.read_line(proc)['error']['code'], -32700)
        self.assertEqual(self.rpc(proc, 'ping')['result'], {})  # still alive after bad input

        proc.stdin.close()
        self.assertEqual(proc.wait(timeout=15), 0)
        self.assertEqual(proc.stdout.read(), b'')  # nothing but protocol replies ever reached stdout
        self.assertNotIn(b'Traceback', proc.stderr.read())

    def test_bad_agent_id_exits_with_usage_code(self):
        proc = self.start('--db', str(self.db), '--agent', 'service:router')
        self.assertEqual(proc.wait(timeout=15), 2)
        self.assertIn(b'service: ids belong to built-in services', proc.stderr.read())

    def test_missing_agent_flag_is_a_usage_error(self):
        proc = self.start('--db', str(self.db))
        self.assertEqual(proc.wait(timeout=15), 2)
        self.assertIn(b'--agent', proc.stderr.read())


if __name__ == '__main__':
    unittest.main()
