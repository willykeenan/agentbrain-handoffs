"""MCP server: one agent's own tools for sending, reading, returning and closing handoffs.

It speaks the Model Context Protocol over stdio (newline-delimited JSON-RPC 2.0), so an
agent app such as Claude Code, Codex or Cursor can launch it for one session with:

    handoffs mcp --agent ID

Why the identity is fixed at start: each agent session launches its own server from
its own client configuration, and no tool takes a sender or "act as" argument. Nothing
a model writes into a tool call can make the server act as another agent. Every change
still goes through Store, which applies the same rules as the command line: only the
recipient accepts or returns work, only the sender closes it, blocked connections stay
blocked.

The server never delivers anything itself. It writes to the inbox, and the delivery
engine (`handoffs run` or `handoffs serve`) turns each handoff into exactly one new turn.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import traceback

from .store import ACTIVE, AGENT_ID, CLOSE_OUTCOMES, PENDING, Store

PROTOCOL_VERSIONS = ('2024-11-05', '2025-03-26', '2025-06-18')
LATEST_PROTOCOL = PROTOCOL_VERSIONS[-1]
SERVER_NAME = 'agentbrain-handoffs'

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

INBOX_LIMIT = 50
STATUS_LIMIT = 20
HISTORY_LIMIT = 8
ENGINE_QUIET_SECONDS = 120  # no engine health report for this long: tell the sender nothing is moving
MAX_DUE_MINUTES = 30 * 24 * 60

# Argument names a model might use to try to speak as someone else. They get a clear refusal.
IDENTITY_WORDS = frozenset({'from', 'sender', 'as', 'as_agent', 'agent', 'agent_id', 'from_agent', 'identity', 'me'})

_ID = {'type': 'string', 'minLength': 1, 'maxLength': 200}

TOOLS = (
    {
        'name': 'send_handoff',
        'title': 'Send a handoff',
        'description': (
            'Send a message or a work assignment to another agent, as {me}. The delivery engine turns it into '
            "exactly one new turn in that agent's own session once it is free: it never interrupts a busy agent "
            'and never sends twice. Give a title to make it tracked work that the other agent returns to you, '
            'and due_minutes for a deadline. Use list_agents for exact agent ids.'),
        'properties': {
            'to': {**_ID, 'description': 'Exact id of the receiving agent, as shown by list_agents. '
                                         'Names are never guessed, so a handoff cannot reach the wrong agent.'},
            'message': {'type': 'string', 'minLength': 1, 'maxLength': 200000,
                        'description': 'Everything the other agent needs to act, in full. '
                                       'It arrives quoted inside a short card.'},
            'title': {'type': 'string', 'minLength': 1, 'maxLength': 200,
                      'description': 'Short title. Giving one makes this tracked work: it stays open until the '
                                     'other agent returns it and you close it.'},
            'due_minutes': {'type': 'integer', 'minimum': 1, 'maximum': MAX_DUE_MINUTES,
                            'description': 'Deadline in minutes (needs a title). If it passes without a return, '
                                           'you get one reminder.'},
            'key': {'type': 'string', 'minLength': 1, 'maxLength': 200,
                    'description': 'Optional idempotency key. Repeating a send with the same key returns the '
                                   'first message instead of sending a second one.'},
        },
        'required': ('to', 'message'),
        'hints': {'readOnlyHint': False, 'destructiveHint': False, 'idempotentHint': False},
    },
    {
        'name': 'inbox',
        'title': 'Your inbox',
        'description': (
            'List messages sent to you ({me}), oldest first: work assignments, returned results and notices. '
            'Shows unread messages unless unread_only is false. Open one in full with read_message.'),
        'properties': {
            'unread_only': {'type': 'boolean', 'default': True,
                            'description': 'true (the default): only messages you have not read yet. '
                                           'false: your most recent messages, read or not.'},
        },
        'required': (),
        'hints': {'readOnlyHint': True},
    },
    {
        'name': 'read_message',
        'title': 'Read a message',
        'description': (
            'Show one message in full, with its work state and the next step. Works for messages you sent or '
            'received. Reading a message sent to you marks it read, so the delivery engine will not start a '
            'separate turn for it.'),
        'properties': {'id': {**_ID, 'description': 'The message id, from inbox or handoff_status.'}},
        'required': ('id',),
        'hints': {'readOnlyHint': False, 'destructiveHint': False, 'idempotentHint': True},
    },
    {
        'name': 'accept_work',
        'title': 'Accept work',
        'description': (
            'Tell the sender you have started a work assignment sent to you. Optional: returning the work also '
            'counts as accepting it.'),
        'properties': {'id': {**_ID, 'description': 'The id of the work message sent to you.'}},
        'required': ('id',),
        'hints': {'readOnlyHint': False, 'destructiveHint': False, 'idempotentHint': True},
    },
    {
        'name': 'return_work',
        'title': 'Return work',
        'description': (
            'Hand a work assignment back to the agent that sent it, with a one-line summary of what you '
            'delivered, or of the exact blocker (blocked=true). The sender receives one return handoff and '
            'records a decision. Ending your turn or reading the message does not return work.'),
        'properties': {
            'id': {**_ID, 'description': 'The id of the work message you were assigned.'},
            'summary': {'type': 'string', 'minLength': 1, 'maxLength': 4000,
                        'description': 'One line: what you delivered, or the exact blocker.'},
            'evidence': {'type': 'string', 'minLength': 1, 'maxLength': 4000,
                         'description': 'Where to verify it: tests run, files changed, commit ids, links.'},
            'blocked': {'type': 'boolean', 'default': False,
                        'description': 'true when you could not finish; the summary then names the blocker.'},
        },
        'required': ('id', 'summary'),
        'hints': {'readOnlyHint': False, 'destructiveHint': False, 'idempotentHint': False},
    },
    {
        'name': 'close_work',
        'title': 'Close work',
        'description': (
            'Record your decision on work you assigned: accepted (done), revision (changes needed) or blocked. '
            'Only the agent that sent the work can close it. Closing does not message the other agent; send a '
            'new handoff to ask for changes.'),
        'properties': {
            'id': {**_ID, 'description': 'The id of the work message you sent. The id of its return message '
                                         'works too.'},
            'outcome': {'type': 'string', 'enum': list(CLOSE_OUTCOMES),
                        'description': 'accepted: done. revision: changes needed. blocked: cannot go further.'},
            'note': {'type': 'string', 'maxLength': 1000, 'description': 'Optional short note for the record.'},
        },
        'required': ('id', 'outcome'),
        'hints': {'readOnlyHint': False, 'destructiveHint': False, 'idempotentHint': True},
    },
    {
        'name': 'handoff_status',
        'title': 'Handoff status',
        'description': (
            "Check delivery progress. With an id: that message's delivery state in plain words, its recent "
            'history and its work state. Without an id: your recent deliveries, sent and received, and the '
            'work still open between you and other agents.'),
        'properties': {'id': {**_ID, 'description': 'Optional message id. Leave it out for an overview.'}},
        'required': (),
        'hints': {'readOnlyHint': True},
    },
    {
        'name': 'list_agents',
        'title': 'List agents',
        'description': (
            'List the registered agents with their exact ids, names and providers, and whether you ({me}) may '
            'send to each.'),
        'properties': {},
        'required': (),
        'hints': {'readOnlyHint': True},
    },
)
TOOL_NAMES = tuple(t['name'] for t in TOOLS)


class RpcError(Exception):
    """A JSON-RPC protocol error (bad method, unknown tool). Tool failures use ToolError."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


class ToolError(Exception):
    """A tool call that cannot be done. The model sees the message with isError: true."""


def _error(request_id, code, message):
    return {'jsonrpc': '2.0', 'id': request_id, 'error': {'code': code, 'message': message}}


def _valid_id(value) -> bool:
    return isinstance(value, str) or (isinstance(value, (int, float)) and not isinstance(value, bool))


def _encode(message) -> str:
    # ASCII-only output: always valid on any pipe, and never contains a raw newline.
    return json.dumps(message, ensure_ascii=True, separators=(',', ':'))


def _when(ts):
    return time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(ts)) if ts else None


def _iso(ts):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ts)) if ts else None


def _preview(text, cap=160) -> str:
    line = ' '.join(str(text or '').split())
    return line if len(line) <= cap else line[:cap - 1] + '…'


def _phase(state) -> str:
    return 'waiting' if state in PENDING else 'in flight' if state in ACTIVE else 'finished'


def _plural(count, word) -> str:
    return str(count) + ' ' + word + ('' if count == 1 else 's')


class McpServer:
    """Answers MCP requests for exactly one agent.

    It knows nothing about pipes, so tests and other hosts can drive it with plain
    dicts (handle) or text lines (handle_line); serve_mcp adds the stdio loop.
    """

    def __init__(self, store: Store, agent_id: str, context_library=None):
        if not isinstance(agent_id, str) or not AGENT_ID.fullmatch(agent_id):
            raise ValueError('Agent id: 1-200 characters, letters, digits and :._@/- only')
        if agent_id.startswith('service:'):
            # The store trusts service: senders without registration or connection checks.
            raise ValueError('service: ids belong to built-in services; start the server with an agent id')
        self.store = store
        self.agent_id = agent_id
        self.protocol = LATEST_PROTOCOL
        # Optional ContextLib plugin (agentbrain-contextlib): the project library's
        # context_* tools, served as this same fixed agent identity.
        self.context_library = context_library

    # ---- JSON-RPC -----------------------------------------------------------------
    def handle_line(self, line):
        """Decode one line, answer it, and encode the reply. None when no reply is due."""
        if isinstance(line, bytes):
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                return _encode(_error(None, PARSE_ERROR, 'Parse error: the line is not valid UTF-8.'))
        line = line.strip().lstrip('﻿')
        if not line:
            return None
        try:
            message = json.loads(line)
        except (ValueError, RecursionError):
            return _encode(_error(None, PARSE_ERROR, 'Parse error: the line is not valid JSON.'))
        reply = self.handle(message)
        return None if reply is None else _encode(reply)

    def handle(self, message):
        """Answer one decoded message or batch. Returns the reply, or None for notifications."""
        if isinstance(message, list):
            if not message:
                return _error(None, INVALID_REQUEST, 'An empty batch is not a request.')
            replies = [r for r in (self._handle_one(m) for m in message) if r is not None]
            return replies or None
        return self._handle_one(message)

    def _handle_one(self, message):
        if not isinstance(message, dict):
            return _error(None, INVALID_REQUEST, 'Each message must be a JSON object.')
        method = message.get('method')
        if method is None and ('result' in message or 'error' in message):
            return None  # a reply to a request; this server never sends requests
        has_id = 'id' in message
        request_id = message.get('id')
        if has_id and not _valid_id(request_id):
            return _error(None, INVALID_REQUEST, 'The id must be a string or a number.')
        if message.get('jsonrpc') != '2.0' or not isinstance(method, str):
            return _error(request_id, INVALID_REQUEST, 'Expected a JSON-RPC 2.0 message with a method name.')
        params = message.get('params')
        params = {} if params is None else params
        if not has_id:
            return None  # notifications (initialized, cancelled, ...) need no reply and change nothing here
        if not isinstance(params, dict):
            return _error(request_id, INVALID_PARAMS, 'params must be a JSON object.')
        try:
            result = self._request(method, params)
        except RpcError as e:
            return _error(request_id, e.code, e.message)
        except Exception as e:  # one bad request must never end the session
            self._log(traceback.format_exc())
            return _error(request_id, INTERNAL_ERROR, 'Internal error: ' + str(e)[:300])
        return {'jsonrpc': '2.0', 'id': request_id, 'result': result}

    def _request(self, method, params):
        if method == 'initialize':
            return self.initialize(params)
        if method == 'ping':
            return {}
        if method == 'tools/list':
            return {'tools': self.tool_list()}
        if method == 'tools/call':
            name = params.get('name')
            if not isinstance(name, str):
                raise RpcError(INVALID_PARAMS, 'tools/call needs the tool name.')
            return self.call_tool(name, params.get('arguments'))
        raise RpcError(METHOD_NOT_FOUND, 'Method not found: ' + method)

    def initialize(self, params):
        """Echo the client's protocol version when supported, otherwise offer the latest."""
        requested = params.get('protocolVersion')
        self.protocol = requested if requested in PROTOCOL_VERSIONS else LATEST_PROTOCOL
        # Read the version from the loaded package instead of importing it, so a package
        # __init__ that imports this module can never create an import cycle.
        version = getattr(sys.modules.get(__package__ or ''), '__version__', None) or '0.0.0'
        return {
            'protocolVersion': self.protocol,
            'capabilities': {'tools': {'listChanged': False}},
            'serverInfo': {'name': SERVER_NAME, 'version': version},
            'instructions': (
                'You are the agent ' + self._label() + '. These tools hand work to other agents and return work '
                'handed to you. Each handoff becomes exactly one new turn in the recipient\'s own session, never '
                'interrupting it. When you receive work, return it with return_work (a summary, or blocked=true '
                'with the exact blocker); ending your turn does not return it.'),
        }

    # ---- tools --------------------------------------------------------------------
    def tool_list(self):
        label = self._label()
        tools = []
        for spec in TOOLS:
            schema = {'type': 'object', 'properties': spec['properties'], 'additionalProperties': False}
            if spec['required']:
                schema['required'] = list(spec['required'])
            tool = {'name': spec['name'], 'description': spec['description'].replace('{me}', label),
                    'inputSchema': schema}
            if self.protocol >= '2025-03-26':
                tool['annotations'] = {'title': spec['title'], **spec['hints'], 'openWorldHint': False}
            if self.protocol >= '2025-06-18':
                tool['title'] = spec['title']
            tools.append(tool)
        return tools + self._context_tools()

    def call_tool(self, name, arguments):
        """Run one tool. Every failure comes back as a result with isError, in plain words."""
        if self.context_library is not None and isinstance(name, str) and name.startswith('context_'):
            return self._context_call(name, arguments)
        spec = next((t for t in TOOLS if t['name'] == name), None)
        if spec is None:
            available = list(TOOL_NAMES) + [t['name'] for t in self._context_tools()]
            raise RpcError(INVALID_PARAMS, 'Unknown tool: ' + name + '. Available: ' + ', '.join(available) + '.')
        try:
            args = self._check_arguments(spec, arguments)
            if name != 'list_agents':
                self._require_registered()
            text, data = getattr(self, '_tool_' + name)(args)
        except (ToolError, ValueError) as e:
            return self._result(str(e), None, error=True)
        except Exception as e:  # an unexpected failure is still an answer, not a dead session
            self._log(traceback.format_exc())
            return self._result('The handoff server hit an unexpected error: ' + str(e)[:300], None, error=True)
        return self._result(text, data)

    # ---- ContextLib plugin -----------------------------------------------------------
    def _context_rpc(self, method, params=None):
        from agentbrain_contextlib import mcp as contextlib_mcp
        message = {'jsonrpc': '2.0', 'id': 0, 'method': method}
        if params is not None:
            message['params'] = params
        reply = contextlib_mcp.handle(self.context_library, self.agent_id, message)
        if reply is None or 'error' in reply:
            error = (reply or {}).get('error') or {}
            raise RpcError(error.get('code', INTERNAL_ERROR), error.get('message', 'ContextLib did not answer'))
        return reply['result']

    def _fit_protocol(self, item, result=False):
        """Drop fields the negotiated protocol version does not define."""
        item = dict(item)
        if self.protocol < '2025-03-26':
            item.pop('annotations', None)
        if self.protocol < '2025-06-18':
            item.pop('structuredContent' if result else 'title', None)
            if not result:
                item.pop('outputSchema', None)
        return item

    def _context_tools(self):
        if self.context_library is None:
            return []
        return [self._fit_protocol(tool) for tool in self._context_rpc('tools/list').get('tools', [])]

    def _context_call(self, name, arguments):
        try:
            result = self._context_rpc('tools/call', {'name': name, 'arguments': arguments or {}})
        except RpcError:
            raise
        except Exception as e:  # the plugin failing is still an answer, not a dead session
            self._log(traceback.format_exc())
            return self._result('The ContextLib plugin hit an unexpected error: ' + str(e)[:300], None, error=True)
        return self._fit_protocol(result, result=True)

    def _result(self, text, data, error=False):
        result = {'content': [{'type': 'text', 'text': text}], 'isError': error}
        if data is not None and self.protocol >= '2025-06-18':
            result['structuredContent'] = data
        return result

    def _check_arguments(self, spec, arguments):
        """Validate against the published schema, with messages a model can act on."""
        arguments = {} if arguments is None else arguments
        if not isinstance(arguments, dict):
            raise ToolError('Arguments must be a JSON object.')
        props = spec['properties']
        unknown = sorted(set(arguments) - set(props))
        if unknown and IDENTITY_WORDS.intersection(unknown):
            raise ToolError('This server always acts as ' + self._label() + '; no tool takes a sender or identity. '
                            'Remove: ' + ', '.join(unknown) + '.')
        if unknown:
            takes = ', '.join(props) if props else 'no arguments'
            raise ToolError('Unknown argument' + ('s' if len(unknown) > 1 else '') + ': ' + ', '.join(unknown) +
                            '. ' + spec['name'] + ' takes ' + takes + '.')
        clean = {}
        for name, prop in props.items():
            value = arguments.get(name)
            if value is None:
                if name in spec['required']:
                    raise ToolError('Missing ' + name + '. ' + prop['description'])
                clean[name] = prop.get('default')
            else:
                clean[name] = self._check_value(name, prop, value)
        return clean

    @staticmethod
    def _check_value(name, prop, value):
        kind = prop['type']
        if kind == 'boolean':
            if not isinstance(value, bool):
                raise ToolError(name + ' must be true or false.')
            return value
        if kind == 'integer':
            if isinstance(value, bool) or not isinstance(value, (int, float)) or \
                    (isinstance(value, float) and not value.is_integer()):
                raise ToolError(name + ' must be a whole number.')
            value = int(value)
            if not prop['minimum'] <= value <= prop['maximum']:
                raise ToolError(name + ' must be between ' + str(prop['minimum']) + ' and ' + str(prop['maximum']) + '.')
            return value
        if not isinstance(value, str):
            raise ToolError(name + ' must be text.')
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise ToolError(name + ' must be valid Unicode text.') from None
        if name in ('to', 'id'):
            value = value.strip()
        if len(value) < prop.get('minLength', 0) or (prop.get('minLength') and not value.strip()):
            raise ToolError(name + ' cannot be empty.')
        if len(value) > prop.get('maxLength', len(value)):
            raise ToolError(name + ' is too long (at most ' + str(prop['maxLength']) + ' characters).')
        if 'enum' in prop and value not in prop['enum']:
            raise ToolError(name + ' must be one of: ' + ', '.join(prop['enum']) + '.')
        return value

    # ---- tool implementations: each returns (text for the model, structured data) --
    def _tool_send_handoff(self, args):
        to = args['to']
        if to == self.agent_id:
            raise ToolError('You cannot send a handoff to yourself.')
        if not self.store.agent(to):
            raise ToolError(self._unknown_agent(to))
        title, due_minutes = args['title'], args['due_minutes']
        if due_minutes is not None and title is None:
            raise ToolError('A deadline needs a title: add a title to make this tracked work.')
        key = None if args['key'] is None else 'mcp:' + self.agent_id + ':' + args['key']
        earlier = self._key_owner(key) if key else None
        mid = self.store.send(self.agent_id, to, args['message'], title=title, key=key,
                              due_seconds=None if due_minutes is None else due_minutes * 60)
        work = self.store.work(mid)
        repeat = earlier == mid
        data = {'messageId': mid, 'to': to, 'toName': self.store.name(to), 'work': bool(work),
                'alreadySent': repeat, 'deliveryPaused': not self.store.settings()['enabled'],
                'engineRunning': self._engine_seen()}
        if work:
            view, words = self._work_view(work, self.agent_id)
            data['title'], data['dueAt'] = view['title'], view['dueAt']
        if repeat:
            lines = ['Already sent earlier with this key, so nothing was sent twice.']
        else:
            what = ' as work "' + work['title'] + '"' if work else ''
            due = ' (due ' + _when(work['created'] + work['due_seconds']) + ')' if work and work['due_seconds'] else ''
            lines = ['Sent to ' + self._who(to) + what + due + '.']
        lines.append('Message id: ' + mid)
        lines += self._delivery_notes(to, data)
        return '\n'.join(lines), data

    def _tool_inbox(self, args):
        unread_only = args['unread_only']
        if unread_only:
            rows = self.store.inbox(self.agent_id, unread_only=True, limit=INBOX_LIMIT)
        else:  # the most recent messages, shown oldest first
            rows = list(reversed(self._rows('SELECT * FROM messages WHERE recipient=? ORDER BY created DESC LIMIT ?',
                                             (self.agent_id, INBOX_LIMIT))))
        described = [self._describe(m) for m in rows]
        data = {'agent': self.agent_id, 'unreadOnly': unread_only, 'messages': [d for d, _, _ in described]}
        if not described:
            return ('No unread messages for ' if unread_only else 'No messages for ') + self._label() + '.', data
        count = _plural(len(described), 'unread message' if unread_only else 'message')
        lines = [count + ' for ' + self._label() + ', oldest first:', '']
        for number, ((item, head, state), m) in enumerate(zip(described, rows), 1):
            details = ['sent ' + _when(m['created'])] + ([state] if state else [])
            if not unread_only and not item['read']:
                details.append('(unread)')
            lines += [str(number) + '. ' + head + ' from ' + self._who(item['from']),
                      '   ' + '; '.join(details),
                      '   id: ' + item['id'],
                      '   "' + item['preview'] + '"']
        if len(described) == INBOX_LIMIT:
            lines += ['', 'Showing ' + str(INBOX_LIMIT) + '; read or return some to see more.']
        lines += ['', 'Open one in full with read_message.']
        return '\n'.join(lines), data

    def _tool_read_message(self, args):
        m = self._visible_message(args['id'])
        marked = m['recipient'] == self.agent_id and not m['read_at']
        if marked:
            self.store.mark_read(m['id'], self.agent_id)
            m = self.store.message(m['id'])
        data, head, state = self._describe(m, full=True)
        data['markedRead'] = marked
        lines = [head, 'From: ' + self._who(m['sender']), 'To: ' + self._who(m['recipient']),
                 'Sent: ' + _when(m['created'])] + (['State: ' + state] if state else []) + ['Id: ' + m['id']]
        work = data.get('work')
        if work and work.get('result'):
            lines.append('Result: ' + ('Blocked: ' if work['result']['disposition'] == 'BLOCKED' else '') +
                         work['result']['summary'])
            if work['result'].get('evidence'):
                lines.append('Evidence: ' + str(work['result']['evidence']))
        if work and work.get('closure'):
            lines.append('Decision: ' + work['closure']['outcome'] +
                         ('. Note: ' + work['closure']['note'] if work['closure'].get('note') else ''))
        lines += ['', '--- message ---', m['body'], '--- end ---']
        step = self._next_step(m, data)
        if step:
            data['nextStep'] = step
            lines += ['', 'Next: ' + step]
        return '\n'.join(lines), data

    def _tool_accept_work(self, args):
        m = self._visible_message(args['id'])
        work = self._work_or_error(m)
        if m['recipient'] != self.agent_id:
            raise ToolError('Only the assigned agent, ' + self._who(m['recipient']) + ', can accept this work.')
        data = {'messageId': m['id'], 'accepted': True}
        if work['returned']:
            return 'You already returned "' + work['title'] + '"; there is nothing left to accept.', data
        self.store.accept(m['id'], self.agent_id)
        text = ('Accepted "' + work['title'] + '" from ' + self._who(m['sender']) + '. When it is done, call '
                'return_work with id ' + m['id'] + ' and a one-line summary (or blocked=true and the exact blocker).')
        return text, data

    def _tool_return_work(self, args):
        m = self._visible_message(args['id'])
        work = self._work_or_error(m)
        rid = self.store.return_work(m['id'], self.agent_id, args['summary'], evidence=args['evidence'],
                                     blocked=args['blocked'])
        how = 'as blocked' if args['blocked'] else 'as done'
        text = ('Returned "' + work['title'] + '" to ' + self._who(m['sender']) + ' ' + how + '.\n'
                'Return message id: ' + rid + '\n' + self.store.name(m['sender']) + ' gets one handoff with your '
                'summary and records a decision.')
        return text, {'messageId': m['id'], 'returnMessageId': rid, 'blocked': args['blocked']}

    def _tool_close_work(self, args):
        m = self._visible_message(args['id'])
        original = self.store.work_for_return(m['id'])
        if original:  # the id of the return message stands for the work it returns
            m = self.store.message(original['message_id'])
        work = self._work_or_error(m)
        if m['sender'] != self.agent_id:
            raise ToolError('Only the agent that sent this work, ' + self._who(m['sender']) + ', can close it.')
        self.store.close_work(m['id'], self.agent_id, args['outcome'], args['note'] or '')
        lines = ['Closed "' + work['title'] + '" as ' + args['outcome'] + '.']
        if not work['returned']:
            lines.append('It was closed before a result was returned.')
        if args['outcome'] == 'revision':
            lines.append(self.store.name(m['recipient']) + ' is not notified by closing; send a new handoff that '
                         'says what to change.')
        return ' '.join(lines), {'messageId': m['id'], 'outcome': args['outcome']}

    def _tool_handoff_status(self, args):
        if args['id'] is not None:
            return self._status_one(self._visible_message(args['id']))
        return self._status_overview()

    def _tool_list_agents(self, args):
        agents = []
        for a in self.store.agents():
            you = a['id'] == self.agent_id
            agents.append({'id': a['id'], 'name': a['name'], 'provider': a['provider'], 'you': you,
                           'canSend': not you and self.store.allowed(self.agent_id, a['id'])})
        data = {'you': self.agent_id, 'agents': agents}
        if not agents:
            return 'No agents are registered yet.', data
        lines = [_plural(len(agents), 'agent') + '. You are ' + self._label() + '.']
        for a in agents:
            note = 'this is you' if a['you'] else 'you can send to it' if a['canSend'] else \
                'you cannot send to it (connection not allowed)'
            lines.append('- ' + a['id'] + ': ' + a['name'] + ' (' + a['provider'] + '), ' + note)
        return '\n'.join(lines), data

    # ---- status -------------------------------------------------------------------
    def _status_one(self, m):
        data, head, state = self._describe(m)
        h = self.store.handoff(m['id'])
        other = 'to ' + self._who(m['recipient']) if m['sender'] == self.agent_id else 'from ' + self._who(m['sender'])
        lines = [head + ' ' + other] + (['State: ' + state] if state else []) + ['Id: ' + m['id']]
        if h:
            events = self.store.events(hid=h['id'])[-HISTORY_LIMIT:]
            resends = int(h['receipt'].get('resends') or 0)
            data['delivery'] = {'state': h['status'], 'phase': _phase(h['status']), 'detail': h['detail'],
                                'attempts': h['attempts'], 'resends': resends,
                                'queuedAt': _iso(h['created']), 'updatedAt': _iso(h['updated']),
                                'history': [{'at': _iso(e['at']), 'state': e['status'], 'detail': e['detail']}
                                            for e in events]}
            lines.append('Delivery: ' + h['status'] + ' (' + _phase(h['status']) + '). ' + h['detail'])
            lines.append('Attempts: ' + str(h['attempts']) + (', resends: ' + str(resends) if resends else ''))
            lines += ['', 'History:'] + ['- ' + _when(e['at']) + ' ' + e['status'] + ': ' + e['detail'] for e in events]
        else:
            if m['intent'] == 'notification':
                why = 'This is a notice. Notices wait in the inbox and never start a turn.'
            elif m['read_at']:
                why = 'It was read before delivery, so no separate turn was needed.'
            elif self._engine_seen():
                why = 'It is in the inbox; the delivery engine will pick it up on its next pass.'
            else:
                why = ('It is waiting in the inbox. No delivery engine has reported in the last 2 minutes; '
                       'start one with `handoffs run` or `handoffs serve`.')
            data['delivery'] = None
            lines.append('Delivery: ' + why)
        return '\n'.join(lines), data

    def _status_overview(self):
        me = self.agent_id
        rows = self._rows('SELECT id, sender, recipient, status, attempts, detail, created, updated FROM handoffs '
                          'WHERE sender=? OR recipient=? ORDER BY created DESC LIMIT ?', (me, me, STATUS_LIMIT))
        deliveries = [{'id': r['id'], 'direction': 'sent' if r['sender'] == me else 'received',
                       'other': r['recipient'] if r['sender'] == me else r['sender'], 'state': r['status'],
                       'phase': _phase(r['status']), 'detail': r['detail'], 'attempts': r['attempts'],
                       'updatedAt': _iso(r['updated'])} for r in rows]
        open_rows = self._rows('SELECT m.id, m.sender, m.recipient FROM work w JOIN messages m ON m.id=w.message_id '
                               'WHERE (m.sender=? OR m.recipient=?) AND w.closed IS NULL ORDER BY w.created LIMIT ?',
                               (me, me, INBOX_LIMIT))
        assigned, mine = [], []
        for r in open_rows:
            view, words = self._work_view(self.store.work(r['id']), r['sender'])
            line = '"' + view['title'] + '" '
            if r['sender'] == me:
                assigned.append((dict(view, to=r['recipient']), line + 'to ' + self._who(r['recipient']) + ': ' + words))
            else:
                mine.append((dict(view, **{'from': r['sender']}), line + 'from ' + self._who(r['sender']) + ': ' + words))
        data = {'deliveries': deliveries, 'workYouAssigned': [v for v, _ in assigned],
                'workAssignedToYou': [v for v, _ in mine]}
        lines = ['Handoffs for ' + self._label() + '.', '']
        if deliveries:
            lines.append('Recent deliveries (newest first):')
            for d in deliveries:
                arrow = 'to ' if d['direction'] == 'sent' else 'from '
                lines.append('- ' + d['id'] + ' ' + arrow + self._who(d['other']) + ': ' + d['state'] + ' (' +
                             d['phase'] + '). ' + _preview(d['detail'], 140))
        else:
            lines.append('No deliveries yet.')
        for heading, items in (('Work you assigned, still open:', assigned), ('Work assigned to you, still open:', mine)):
            if items:
                lines += ['', heading] + ['- ' + v['id'] + ' ' + words for v, words in items]
        if not assigned and not mine:
            lines += ['', 'No open work.']
        return '\n'.join(lines), data

    # ---- helpers ------------------------------------------------------------------
    def _label(self) -> str:
        agent = self.store.agent(self.agent_id)
        return self._who(self.agent_id) if agent else self.agent_id

    def _who(self, agent_id) -> str:
        name = self.store.name(agent_id)
        return agent_id if name == agent_id else name + ' (' + agent_id + ')'

    def _require_registered(self):
        if not self.store.agent(self.agent_id):
            raise ToolError('This server acts as the agent "' + self.agent_id + '", which is not registered in this '
                            'handoff database. Register it with `handoffs agent add ' + self.agent_id + ' ...`, or '
                            'start the server with the right --agent and --db.')

    def _unknown_agent(self, agent_id) -> str:
        wanted = agent_id.casefold()
        close = [a for a in self.store.agents() if wanted in (a['id'].casefold(), a['name'].casefold())]
        text = 'No agent has the id "' + agent_id + '".'
        if close:
            text += ' Did you mean ' + ' or '.join('"' + a['id'] + '" (' + a['name'] + ')' for a in close) + '?'
        return text + ' Ids must match exactly; list_agents shows them.'

    def _visible_message(self, mid):
        """The message, if this agent sent or received it. Others' messages look the same as unknown ones."""
        try:
            m = self.store.message(mid)
        except ValueError:
            m = None
        if not m or self.agent_id not in (m['sender'], m['recipient']):
            raise ToolError('No message ' + mid + ' was sent to or by you. Check the id with inbox or handoff_status.')
        return m

    def _work_or_error(self, m):
        work = self.store.work(m['id'])
        if not work:
            if self.store.work_for_return(m['id']) is None:
                raise ToolError('Message ' + m['id'] + ' is a plain message, not a work assignment.')
            raise ToolError('Message ' + m['id'] + ' is a returned result, not the work itself.')
        return work

    def _work_view(self, work, sender):
        """(data, words) for a work contract; words read like 'accepted, in progress, due ...'."""
        now = self.store.clock()
        due = work['created'] + work['due_seconds'] if work.get('due_seconds') else None
        result, closure = work.get('result') or {}, work.get('closure') or {}
        if work.get('closed'):
            state, words = 'closed', 'closed as ' + str(closure.get('outcome'))
        elif work.get('returned'):
            state = 'returned'
            decider = 'your' if sender == self.agent_id else self.store.name(sender) + "'s"
            words = ('returned as blocked' if result.get('disposition') == 'BLOCKED' else 'returned') + \
                ', waiting for ' + decider + ' decision'
        elif work.get('accepted'):
            state, words = 'accepted', 'accepted, in progress'
        else:
            state, words = 'open', 'not started yet'
        overdue = bool(due and state in ('open', 'accepted') and now >= due)
        view = {'id': work['message_id'], 'title': work['title'], 'state': state, 'dueAt': _iso(due), 'overdue': overdue}
        if result:
            view['result'] = {k: result.get(k) for k in ('summary', 'disposition', 'evidence')}
        if closure:
            view['closure'] = {k: closure.get(k) for k in ('outcome', 'note')}
        if due and state in ('open', 'accepted'):
            words += ', due ' + _when(due) + (' (overdue)' if overdue else '')
        return view, words

    def _describe(self, m, full=False):
        """(data, headline, state words or None) for one message as this agent sees it."""
        work = self.store.work(m['id'])
        returned = None if work else self.store.work_for_return(m['id'])
        data = {'id': m['id'], 'from': m['sender'], 'fromName': self.store.name(m['sender']), 'to': m['recipient'],
                'toName': self.store.name(m['recipient']), 'sentAt': _iso(m['created']), 'read': bool(m['read_at'])}
        words = None
        if work:
            view, words = self._work_view(work, m['sender'])
            data.update(kind='work', work=view)
            head = 'Work "' + work['title'] + '"' + (', ' + words if words else '')
        elif returned:  # a result coming back: the work's sender is this message's recipient
            view, words = self._work_view(returned, m['recipient'])
            data.update(kind='return', returnFor=returned['message_id'], work=view)
            head = 'Result for "' + returned['title'] + '"' + (', ' + words if words else '')
        elif m['intent'] == 'notification':
            data['kind'], head = 'notification', 'Notice'
        else:
            data['kind'], head = 'message', 'Message'
        if full:
            data['body'] = m['body']
        else:
            data['preview'] = _preview(m['body'])
        return data, head, words

    def _next_step(self, m, data):
        work, me = data.get('work'), self.agent_id
        if data['kind'] == 'work' and m['recipient'] == me:
            if work['state'] in ('returned', 'closed'):
                return 'You returned this work; ' + self._who(m['sender']) + ' decides what happens next.'
            return ('Do the work, then call return_work with id ' + m['id'] + ' and a one-line summary '
                    '(or blocked=true and the exact blocker).')
        if data['kind'] == 'work' and m['sender'] == me:
            if work['state'] == 'returned':
                return 'Record your decision with close_work (id ' + m['id'] + ', outcome accepted, revision or blocked).'
            if work['state'] != 'closed':
                return 'Waiting for ' + self._who(m['recipient']) + ' to return it.'
        if data['kind'] == 'return' and m['recipient'] == me and work['state'] != 'closed':
            return ('Record your decision with close_work (id ' + data['returnFor'] +
                    ', outcome accepted, revision or blocked).')
        return None

    def _delivery_notes(self, to, data):
        if data['alreadySent']:
            return ['Check progress with handoff_status.']
        if data['deliveryPaused']:
            return ['Delivery is paused right now; the message waits in the inbox until it is resumed.']
        if not data['engineRunning']:
            return ['No delivery engine has reported in the last 2 minutes, so it is not delivered until one runs '
                    '(`handoffs run` or `handoffs serve`). The message is safe in the inbox meanwhile.']
        return ['The delivery engine starts one new turn in ' + self.store.name(to) + "'s session when it is free, "
                'without interrupting it. Check progress with handoff_status.']

    def _engine_seen(self) -> bool:
        """True when the engine wrote its health file recently (the engine's default location)."""
        path = self.store.path.with_name(self.store.path.name + '.health.json')
        try:
            at = float(json.loads(path.read_text())['at'])
        except Exception:
            return False
        return self.store.clock() - at <= ENGINE_QUIET_SECONDS

    def _key_owner(self, key):
        rows = self._rows('SELECT id FROM messages WHERE key=?', (key,))
        return rows[0]['id'] if rows else None

    def _rows(self, sql, params):
        # Read-only queries the Store has no method for (per-agent history, key lookup).
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(sql, params)]

    @staticmethod
    def _log(text):
        print(text.rstrip(), file=sys.stderr, flush=True)


# ---- stdio -------------------------------------------------------------------------
def _write(stream, text):
    if isinstance(stream, io.TextIOBase):
        stream.write(text + '\n')
    else:
        stream.write((text + '\n').encode('ascii'))
    stream.flush()


def open_context_library(path):
    """The ContextLib library at `path` for the plugin, or None when no path is given."""
    if not path:
        return None
    try:
        from agentbrain_contextlib.library import Library
    except ImportError:
        raise ValueError('a ContextLib library was given but agentbrain-contextlib is not installed; '
                         'pip install "git+https://github.com/willykeenan/agentbrain-contextlib"')
    if not os.path.isfile(os.path.join(path, 'library.json')):
        raise ValueError('no ContextLib library at ' + path + ' (run: ctxlib init PATH)')
    return Library(path, writer='mcp')


def serve_mcp(store: Store, agent_id: str, stdin=None, stdout=None, context_library=None) -> int:
    """Serve one agent over stdio until the client closes stdin. Returns the exit code.

    Standard output carries protocol messages only. While serving on the real stdout,
    sys.stdout points at stderr, so a stray print() anywhere cannot corrupt the stream
    the client is parsing.
    """
    server = McpServer(store, agent_id, context_library=context_library)
    saved_stdout = sys.stdout
    if stdout is None:
        stdout = getattr(sys.stdout, 'buffer', sys.stdout)
        sys.stdout = sys.stderr
    if stdin is None:
        stdin = getattr(sys.stdin, 'buffer', sys.stdin)
    try:
        while True:
            line = stdin.readline()
            if not line:
                return 0
            reply = server.handle_line(line)
            if reply is not None:
                _write(stdout, reply)
    except (BrokenPipeError, KeyboardInterrupt):
        return 0
    finally:
        sys.stdout = saved_stdout


def main(argv=None) -> int:
    """`python -m agentbrain_handoffs.mcp --agent ID [--db PATH]`, the same server as `handoffs mcp`."""
    parser = argparse.ArgumentParser(prog='python -m agentbrain_handoffs.mcp',
                                     description='Run the handoffs MCP server over stdio for one agent.')
    parser.add_argument('--agent', required=True, help='the exact agent id this server acts as')
    parser.add_argument('--db', help='handoff database (default: $HANDOFFS_DB, else ./.handoffs/handoffs.sqlite3)')
    parser.add_argument('--context-library', help='also serve this ContextLib library\'s context_* tools '
                                                  '(default: $CONTEXTLIB_ROOT when set)')
    args = parser.parse_args(argv)
    db = args.db or os.environ.get('HANDOFFS_DB') or os.path.join('.handoffs', 'handoffs.sqlite3')
    try:
        library = open_context_library(args.context_library or os.environ.get('CONTEXTLIB_ROOT'))
        return serve_mcp(Store(db), args.agent, context_library=library)
    except ValueError as e:
        print('handoffs mcp: ' + str(e), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
