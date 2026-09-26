"""The optional ContextLib plugin: context_* tools on the same MCP server, as the same agent."""
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from agentbrain_handoffs.mcp import McpServer, RpcError, open_context_library
from agentbrain_handoffs.store import Store


def _call(server, name, arguments, id_=1):
    return server.handle({'jsonrpc': '2.0', 'id': id_, 'method': 'tools/call',
                          'params': {'name': name, 'arguments': arguments}})


class _Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name).resolve()
        self.store = Store(str(self.tmp / 'handoffs.sqlite3'))
        self.store.register_agent('codex:builder', name='Builder', provider='ready')


class TestPluginWithStub(_Base):
    """Always runs: a stand-in ContextLib module proves the wiring and the identity."""

    def setUp(self):
        super().setUp()
        self.seen = []

        def handle(library, identity, message):
            self.seen.append((library, identity, message['method'], message.get('params')))
            if message['method'] == 'tools/list':
                return {'jsonrpc': '2.0', 'id': 0, 'result': {'tools': [
                    {'name': 'context_brief', 'title': 'Brief', 'description': 'd',
                     'inputSchema': {'type': 'object'}, 'annotations': {'readOnlyHint': True}}]}}
            return {'jsonrpc': '2.0', 'id': 0, 'result': {
                'content': [{'type': 'text', 'text': 'brief for ' + identity}], 'isError': False,
                'structuredContent': {'ok': True}}}

        package = types.ModuleType('agentbrain_contextlib')
        module = types.ModuleType('agentbrain_contextlib.mcp')
        module.handle = handle
        package.mcp = module
        patcher = mock.patch.dict(sys.modules, {'agentbrain_contextlib': package, 'agentbrain_contextlib.mcp': module})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_no_library_means_no_context_tools(self):
        server = McpServer(self.store, 'codex:builder')
        names = [t['name'] for t in server.tool_list()]
        self.assertFalse([n for n in names if n.startswith('context_')])
        self.assertEqual(self.seen, [])

    def test_context_tools_are_listed_and_called_as_the_server_identity(self):
        library = object()
        server = McpServer(self.store, 'codex:builder', context_library=library)
        self.assertIn('context_brief', [t['name'] for t in server.tool_list()])
        reply = _call(server, 'context_brief', {'project': 'demo', 'author': 'human:someone-else'})
        self.assertFalse(reply['result']['isError'])
        self.assertEqual(reply['result']['content'][0]['text'], 'brief for codex:builder')
        calls = [s for s in self.seen if s[2] == 'tools/call']
        self.assertEqual(calls[-1][0], library)
        self.assertEqual(calls[-1][1], 'codex:builder', 'the plugin always acts as the fixed agent')

    def test_older_protocols_get_only_fields_they_define(self):
        server = McpServer(self.store, 'codex:builder', context_library=object())
        server.handle({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
                       'params': {'protocolVersion': '2024-11-05', 'capabilities': {}, 'clientInfo': {'name': 't', 'version': '1'}}})
        tool = next(t for t in server.tool_list() if t['name'] == 'context_brief')
        self.assertNotIn('annotations', tool)
        self.assertNotIn('title', tool)
        result = _call(server, 'context_brief', {'project': 'demo'})['result']
        self.assertNotIn('structuredContent', result)


class TestOpenLibrary(unittest.TestCase):
    def test_no_path_means_no_plugin(self):
        self.assertIsNone(open_context_library(None))
        self.assertIsNone(open_context_library(''))

    def test_missing_library_explains_itself(self):
        with tempfile.TemporaryDirectory() as tmp:
            try:
                import agentbrain_contextlib  # noqa: F401
            except ImportError:
                with self.assertRaises(ValueError) as caught:
                    open_context_library(tmp)
                self.assertIn('pip install', str(caught.exception))
            else:
                with self.assertRaises(ValueError) as caught:
                    open_context_library(tmp)
                self.assertIn('ctxlib init', str(caught.exception))


try:
    import agentbrain_contextlib.library  # noqa: F401
    HAVE_CONTEXTLIB = True
except ImportError:
    HAVE_CONTEXTLIB = False


@unittest.skipUnless(HAVE_CONTEXTLIB, 'agentbrain-contextlib is not installed')
class TestPluginWithContextLib(_Base):
    """Runs when agentbrain-contextlib is installed: the real library, end to end."""

    def test_record_through_the_handoffs_server_is_authored_by_the_agent(self):
        from agentbrain_contextlib.library import Library
        root = self.tmp / 'library'
        Library.init(root).create_project('demo', 'Demo', purpose='Plugin test')
        server = McpServer(self.store, 'codex:builder', context_library=open_context_library(str(root)))
        reply = _call(server, 'context_record', {'project': 'demo', 'type': 'lesson', 'title': 'Plugin works',
                                                 'body': '**Lesson:** one server, one identity.'})
        self.assertFalse(reply['result']['isError'], reply)
        made = json.loads(reply['result']['content'][0]['text'])
        stored = Library(root).get('demo', made['id'])['meta']
        self.assertEqual(stored['author'], 'codex:builder')
        brief = _call(server, 'context_brief', {'project': 'demo'}, id_=2)
        self.assertFalse(brief['result']['isError'])


if __name__ == '__main__':
    unittest.main()
