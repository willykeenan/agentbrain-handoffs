"""The ``handoffs`` command line: register agents, send work and run the delivery engine.

Every command is a thin layer over :class:`Store` and :class:`Engine`, so the
terminal, the MCP server and the web page always see the same state. Commands that
need optional parts (the adapters, the web page, the MCP server) import them only
when they run, so a problem in one of those never breaks the rest of the CLI.

Exit codes: 0 success, 1 the operation was refused or failed, 2 a usage error.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from pathlib import Path

from . import __version__, cards
from .store import ACTIVE, CLOSE_OUTCOMES, DEFAULT_SETTINGS, TERMINAL, Store

DEFAULT_DB = os.path.join('.handoffs', 'handoffs.sqlite3')
PROVIDERS = ('demo', 'command', 'codex', 'claude-code')  # keep in step with transports.available()
ATTENTION = ('HELD', 'UNAVAILABLE', 'OWNER_REJECTED', 'UNCERTAIN', 'FAILED')
ENGINE_FRESH_SECONDS = 60


class UsageError(Exception):
    """The command line itself was wrong (exit code 2)."""


# ---- small helpers -----------------------------------------------------------------

def db_path(args) -> Path:
    """--db wins, then HANDOFFS_DB, then ./.handoffs/handoffs.sqlite3."""
    return Path(os.path.expanduser(getattr(args, 'db', None) or os.environ.get('HANDOFFS_DB') or DEFAULT_DB))


def open_store(args) -> Store:
    """Open an existing database. Only `init` creates one.

    A mistyped path or a different working directory must not quietly start a second,
    empty database: agents would then hand off into different worlds.
    """
    path = db_path(args)
    if not path.exists():
        raise ValueError('No handoff database at ' + str(path) + '. Create it with "handoffs init", '
                         'or point to yours with --db PATH or HANDOFFS_DB.')
    return Store(path)


def json_value(text):
    """Parse a value as JSON when it is JSON (true, 3, [..]); otherwise keep the plain string."""
    try:
        return json.loads(text)
    except ValueError:
        return text


def parse_sets(items) -> dict:
    values = {}
    for item in items or []:
        key, sep, raw = item.partition('=')
        if not sep or not key.strip():
            raise UsageError('--set needs KEY=VALUE, got ' + repr(item))
        values[key.strip()] = json_value(raw)
    return values


def actor(args) -> str:
    """The agent a command acts for: --as, else HANDOFFS_AGENT. Never guessed."""
    who = getattr(args, 'actor', None) or os.environ.get('HANDOFFS_AGENT')
    if not who:
        raise UsageError('Say which agent you are acting as with --as AGENT_ID (or set HANDOFFS_AGENT). '
                         'A handoff card names you on its "Exact recipient:" line.')
    return who


def resolve_message(store: Store, ref: str) -> str:
    """Accept a full message id or a unique prefix of at least 6 characters."""
    ref = (ref or '').strip()
    with store.connect() as db:
        if db.execute('SELECT 1 FROM messages WHERE id=?', (ref,)).fetchone():
            return ref
        rows = db.execute('SELECT id FROM messages WHERE substr(id,1,?)=? LIMIT 2', (len(ref), ref)).fetchall() \
            if len(ref) >= 6 else []
    if len(rows) == 1:
        return rows[0]['id']
    if len(rows) > 1:
        raise ValueError('Several messages start with ' + ref + '; use more of the id.')
    raise ValueError('Unknown message ' + ref)


def require_agent(store: Store, agent_id: str) -> dict:
    agent = store.agent(agent_id)
    if not agent:
        raise ValueError('Unknown agent ' + agent_id + '. See "handoffs agent list".')
    return agent


def span(seconds) -> str:
    seconds = abs(int(seconds))
    if seconds < 60:
        return str(seconds) + ' s'
    if seconds < 3600:
        return str(seconds // 60) + ' min'
    if seconds < 86400:
        return str(seconds // 3600) + ' h ' + str(seconds % 3600 // 60) + ' min'
    return str(seconds // 86400) + ' d'


def ago(at, now) -> str:
    return 'just now' if now - at < 2 else span(now - at) + ' ago'


def utc(at) -> str:
    return time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(at))


def clip(text, width) -> str:
    text = ' '.join(str(text or '').split())
    return text if len(text) <= width else text[:width - 1] + '…'


def table(rows, headers) -> str:
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    line = lambda cells: '  ' + '  '.join(c.ljust(w) for c, w in zip(cells, widths)).rstrip()  # noqa: E731
    return '\n'.join([line(headers)] + [line(r) for r in rows])


def out(args, data, human):
    """Print JSON with --json, else the human text."""
    if getattr(args, 'json', False):
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
    else:
        print(human)


def engine_health(store: Store, now=None) -> dict:
    """What the last engine pass wrote, plus whether it looks alive."""
    now = time.time() if now is None else now
    path = store.path.with_name(store.path.name + '.health.json')
    try:
        health = json.loads(path.read_text())
    except (OSError, ValueError):
        health = {}
    at = health.get('at')
    running = isinstance(at, (int, float)) and now - at < ENGINE_FRESH_SECONDS
    return {'running': bool(running), 'lastPassAt': at, 'health': health}


def work_state(w, now) -> str:
    if w.get('closed'):
        return 'closed: ' + ((w.get('closure') or {}).get('outcome') or 'done')
    if w.get('returned'):
        blocked = (w.get('result') or {}).get('disposition') == 'BLOCKED'
        return ('returned blocked' if blocked else 'returned') + ', waiting for a decision'
    if w.get('due_seconds'):
        left = w['created'] + w['due_seconds'] - now
        return ('due in ' + span(left)) if left >= 0 else ('overdue by ' + span(left))
    return 'open, no deadline'


# ---- engine wiring -----------------------------------------------------------------

def make_router(store: Store, speed=1.0):
    """Every shipped adapter. The demo one shares this store so its agents can return work;
    the others get it so run records live next to the database and turns get HANDOFFS_DB.

    An adapter that fails to load is left out with a note, which leaves its agents
    UNAVAILABLE (nothing is ever sent blind) instead of stopping all delivery.
    """
    from .transport import Router
    from .transports import available
    router = Router()
    for provider, factory in available().items():
        try:
            router.register(provider, factory(store=store, speed=speed) if provider == 'demo' else factory(store=store))
        except Exception as e:  # noqa: BLE001 (one broken adapter must not stop the others)
            print('handoffs: the ' + provider + ' adapter could not load (' + str(e)[:200] + '); '
                  'its agents will show as unavailable.', file=sys.stderr)
    return router


def build_engine(store: Store, speed=1.0, one_shot=False):
    from . import outage
    from .engine import Engine
    guard = outage.from_settings(store.settings().get('outageGuard'))
    return Engine(store, make_router(store, speed=speed), outage=guard, one_shot=one_shot)


# ---- commands: setup ---------------------------------------------------------------

def cmd_init(args):
    path = db_path(args)
    existed = path.exists()
    Store(path)
    if existed:
        out(args, {'db': str(path), 'created': False}, 'Using the existing handoff database at ' + str(path) + '.')
        return 0
    out(args, {'db': str(path), 'created': True}, '\n'.join([
        'Created a handoff database at ' + str(path) + '.',
        '',
        'Next steps:',
        '  handoffs agent add planner --provider demo',
        '  handoffs agent add builder --provider demo',
        '  handoffs send planner builder "Add a health check" --title "Health check" --due-minutes 30',
        '  handoffs serve        # delivery engine plus a live page on http://127.0.0.1:8765/',
        '',
        'Every agent must use this same database: pass --db ' + str(path.resolve()) + ' or set HANDOFFS_DB.',
    ]))
    return 0


def check_agent(provider, endpoint, cwd, settings):
    """Refuse setups that could never deliver, before any handoff waits on them."""
    if provider in ('codex', 'claude-code') and not endpoint:
        raise UsageError(provider + ' agents need --endpoint: the id of the existing session to deliver into.')
    if provider == 'claude-code' and not cwd:
        raise UsageError('claude-code agents need --cwd: the project folder that session belongs to.')
    if provider == 'command':
        command, url = settings.get('command'), settings.get('url')
        if command is None and not url:
            raise UsageError('command agents need --set \'command=["./deliver.sh", "{text_file}"]\' '
                             'or --set url=https://... (see docs/ADAPTERS.md).')
        if command is not None and (not isinstance(command, list) or not command
                                    or not all(isinstance(part, str) for part in command)):
            raise UsageError('command must be a JSON list of strings, for example '
                             '--set \'command=["./deliver.sh", "{text_file}"]\'.')


def cmd_agent_add(args):
    store = open_store(args)
    old = store.agent(args.id)
    if not old and not args.provider:
        raise UsageError('A new agent needs --provider (' + ', '.join(PROVIDERS) + ').')
    settings = {**(old['settings'] if old else {}), **parse_sets(args.set)}
    provider = args.provider or old['provider']
    endpoint = args.endpoint if args.endpoint is not None else (old['endpoint'] if old else '')
    cwd = os.path.abspath(os.path.expanduser(args.cwd)) if args.cwd else (old['cwd'] if old else '')
    check_agent(provider, endpoint, cwd, settings)
    name = args.name or (old['name'] if old else None)
    agent = store.register_agent(args.id, name=name, provider=provider, endpoint=endpoint, cwd=cwd, settings=settings)
    verb = 'Updated' if old else 'Added'
    note = ''
    if old and (old['endpoint'], old['provider']) != (agent['endpoint'], agent['provider']):
        note = '\nHandoffs queued for the old endpoint will show as unavailable; none are redirected.'
    out(args, agent, verb + ' agent ' + agent['id'] + ' (' + agent['name'] + ', ' + agent['provider'] + ').' + note)
    return 0


def cmd_agent_missing(args):
    raise UsageError('Choose an action: handoffs agent add|list|remove (see handoffs agent --help).')


def cmd_agent_list(args):
    store = open_store(args)
    agents = store.agents()
    if not agents:
        out(args, [], 'No agents yet. Add one with: handoffs agent add ID --provider demo')
        return 0
    rows = [[a['id'], a['name'], a['provider'], clip(a['endpoint'] or '-', 40)] for a in agents]
    out(args, agents, table(rows, ['ID', 'NAME', 'PROVIDER', 'ENDPOINT']))
    return 0


def cmd_agent_remove(args):
    store = open_store(args)
    require_agent(store, args.id)
    store.remove_agent(args.id)
    out(args, {'removed': args.id}, 'Removed agent ' + args.id + '. Handoffs still addressed to it will show as '
                                    'unavailable; nothing is redirected to another agent.')
    return 0


def _connection(args, allow):
    store = open_store(args)
    for agent_id in (args.sender, args.recipient):
        if not agent_id.startswith('service:'):
            require_agent(store, agent_id)
    if args.sender == args.recipient:
        raise UsageError('An agent cannot hand off to itself.')
    store.set_connection(args.sender, args.recipient, allow)
    text = args.sender + ' → ' + args.recipient + ': ' + ('allowed' if allow else 'blocked') + '.'
    if allow and store.settings()['connections'] == 'open':
        text += ' (Connections are open, so every pair is allowed unless blocked; ' \
                '"handoffs config connections explicit" makes allows required.)'
    if not allow:
        text += ' Queued handoffs on this connection are held, not sent.'
    out(args, {'sender': args.sender, 'recipient': args.recipient, 'allow': allow}, text)
    return 0


def cmd_allow(args):
    return _connection(args, True)


def cmd_block(args):
    return _connection(args, False)


def cmd_config(args):
    store = open_store(args)
    settings = store.settings()
    if args.key is None:
        out(args, settings, '\n'.join(k + ' = ' + json.dumps(v) for k, v in settings.items()))
        return 0
    if args.key not in DEFAULT_SETTINGS:
        raise UsageError('Unknown setting ' + args.key + '. Settings: ' + ', '.join(DEFAULT_SETTINGS) + '.')
    if args.value is None:
        out(args, {args.key: settings[args.key]}, json.dumps(settings[args.key]))
        return 0
    value = time.time() if (args.key, args.value) == ('enabledAfter', 'now') else json_value(args.value)
    if args.key == 'enabled' and not isinstance(value, bool):
        raise UsageError('enabled must be true or false.')
    if args.key == 'enabledAfter' and (isinstance(value, bool) or not isinstance(value, (int, float))):
        raise UsageError('enabledAfter must be a Unix time in seconds, or "now".')
    if args.key == 'outageGuard':
        from . import outage
        try:
            outage.from_settings(value)
        except (TypeError, ValueError) as e:
            raise UsageError(str(e)) from None
    settings = store.configure(**{args.key: value})
    out(args, settings, args.key + ' = ' + json.dumps(settings[args.key]))
    return 0


# ---- commands: messages and work ---------------------------------------------------

def cmd_send(args):
    store = open_store(args)
    body = sys.stdin.read() if args.message == '-' else args.message
    due_seconds = None
    if args.due_minutes is not None:
        if args.title is None:
            raise UsageError('--due-minutes needs --title: only work contracts have deadlines.')
        if not 1 <= args.due_minutes <= 43200:
            raise UsageError('--due-minutes must be between 1 and 43200 (30 days).')
        due_seconds = args.due_minutes * 60
    repeat = False
    if args.key is not None:
        with store.connect() as db:
            repeat = db.execute('SELECT 1 FROM messages WHERE key=?', (args.key,)).fetchone() is not None
    mid = store.send(args.sender, args.recipient, body, key=args.key, title=args.title, due_seconds=due_seconds)
    work = store.work(mid)
    lines = [('Already sent with this key: ' if repeat else 'Sent ') + mid + ' (' + store.name(args.sender) + ' → '
             + store.name(args.recipient) + ').']
    if work:
        lines.append('Work: "' + work['title'] + '"' + (', due ' + utc(work['created'] + work['due_seconds'])
                                                         if work['due_seconds'] else '') + '.')
    if not engine_health(store)['running']:
        lines.append('Delivery happens when the engine runs: "handoffs serve", "handoffs run" or one "handoffs tick".')
    out(args, {'id': mid, 'repeat': repeat, 'work': work}, '\n'.join(lines))
    return 0


def cmd_inbox(args):
    store = open_store(args)
    agent = require_agent(store, args.agent)
    messages = store.inbox(args.agent, unread_only=not args.all, limit=args.limit)
    now = time.time()
    for m in messages:
        m['senderName'] = store.name(m['sender'])
        m['work'] = store.work(m['id'])
    if not messages:
        out(args, [], 'No ' + ('' if args.all else 'unread ') + 'messages for ' + agent['name'] + '.')
        return 0
    noun = 'message' if len(messages) == 1 else 'messages'
    lines = [str(len(messages)) + ('' if args.all else ' unread') + ' ' + noun + ' for ' + agent['name']
             + ' (' + agent['id'] + ')', '']
    for m in messages:
        facts = ['from ' + m['senderName'], ago(m['created'], now)]
        if m['work']:
            facts += ['work "' + m['work']['title'] + '"', work_state(m['work'], now)]
        if m['intent'] == 'notification':
            facts.append('notification')
        if m['read_at']:
            facts.append('read')
        lines += ['  ' + m['id'], '  ' + ' · '.join(facts), '  ' + clip(m['body'], 100), '']
    lines.append('Read one with: handoffs read ID --as ' + agent['id'])
    out(args, messages, '\n'.join(lines))
    return 0


def cmd_read(args):
    store = open_store(args)
    who = actor(args)
    mid = resolve_message(store, args.id)
    m = store.message(mid)
    if who not in (m['sender'], m['recipient']):
        raise ValueError('Only the sender or the recipient can read this message.')
    if who == m['recipient']:
        store.mark_read(mid, who)
        m = store.message(mid)
    work, returned_for = store.work(mid), store.work_for_return(mid)
    now = time.time()
    lines = ['Message ' + mid,
             'From: ' + store.name(m['sender']) + ' (' + m['sender'] + ')',
             'To:   ' + store.name(m['recipient']) + ' (' + m['recipient'] + ')',
             'Sent: ' + utc(m['created']) + ' (' + ago(m['created'], now) + ')']
    if work:
        lines.append('Work: "' + work['title'] + '", ' + work_state(work, now))
        if who == m['recipient'] and not work['returned']:
            lines.append('When done: handoffs return ' + mid + ' --as ' + who + ' --summary "what you did"')
    if returned_for:  # a return message goes back to the original sender, who decides
        lines.append('This returns the work "' + returned_for['title'] + '"; decide with: handoffs close '
                     + returned_for['message_id'] + ' --as ' + m['recipient'] + ' accepted|revision|blocked')
    lines += ['', m['body']]
    out(args, {**m, 'work': work, 'returns': returned_for}, '\n'.join(lines))
    return 0


def cmd_accept(args):
    store = open_store(args)
    mid = resolve_message(store, args.id)
    store.accept(mid, actor(args))
    work = store.work(mid)
    out(args, work, 'Accepted "' + work['title'] + '".')
    return 0


def cmd_return(args):
    store = open_store(args)
    mid = resolve_message(store, args.id)
    rid = store.return_work(mid, actor(args), args.summary, evidence=args.evidence, blocked=args.blocked)
    work = store.work(mid)
    sender = store.message(mid)['sender']
    out(args, {'work': work, 'returnMessage': rid},
        ('Reported a blocker on "' if args.blocked else 'Returned "') + work['title'] + '" to ' + store.name(sender)
        + '. They get a return handoff to review and close.')
    return 0


def cmd_close(args):
    store = open_store(args)
    mid = resolve_message(store, args.id)
    store.close_work(mid, actor(args), args.outcome, args.note or '')
    work = store.work(mid)
    out(args, work, 'Closed "' + work['title'] + '": ' + args.outcome + '.')
    return 0


# ---- commands: status and the engine -----------------------------------------------

def needs_attention(h, now) -> bool:
    """A person should look: held, unreachable, refused, unknown, or failed within the last day."""
    return h['status'] in ATTENTION and (h['status'] != 'FAILED' or now - h['updated'] < 86400)


def status_report(store: Store, now=None) -> dict:
    """One snapshot for `handoffs status`: agents, deliveries, open work and engine health."""
    now = time.time() if now is None else now
    with store.connect() as db:
        counts = {r['status']: r['n'] for r in db.execute('SELECT status, COUNT(*) AS n FROM handoffs GROUP BY status')}
        waiting = db.execute("SELECT COUNT(*) FROM messages m LEFT JOIN handoffs h ON h.id=m.id WHERE h.id IS NULL "
                             "AND m.intent='handoff' AND m.read_at IS NULL AND m.created>=?",
                             (store.settings()['enabledAfter'],)).fetchone()[0]
        rows = [dict(r) for r in db.execute(
            'SELECT h.id, h.sender, h.recipient, h.status, h.detail, h.attempts, h.created, h.updated, m.body, '
            'w.title AS work_title, r.title AS returned_title FROM handoffs h JOIN messages m ON m.id=h.id '
            'LEFT JOIN work w ON w.message_id=h.id LEFT JOIN work r ON r.return_message_id=h.id '
            'ORDER BY h.created DESC LIMIT 200')]
    work = store.work_list(200)
    open_load = {}
    for w in work:
        if not w['returned'] and not w['closed']:
            open_load[w['recipient']] = open_load.get(w['recipient'], 0) + 1
    names = {a['id']: a['name'] for a in store.agents()}
    handoffs = []
    for r in rows:
        title = r['work_title'] or ('Returned: ' + r['returned_title'] if r['returned_title'] else cards.subject(r['body']))
        handoffs.append({'id': r['id'], 'title': title, 'sender': r['sender'], 'recipient': r['recipient'],
                         'senderName': names.get(r['sender']) or store.name(r['sender']),
                         'recipientName': names.get(r['recipient']) or store.name(r['recipient']),
                         'status': r['status'], 'detail': r['detail'], 'attempts': r['attempts'],
                         'created': r['created'], 'updated': r['updated'], 'needsAttention': needs_attention(r, now)})
    for w in work:
        w['state'] = work_state(w, now)
        w['overdue'] = bool(w['due_seconds'] and not w['returned'] and not w['closed']
                            and now > w['created'] + w['due_seconds'])
    return {'db': str(store.path), 'generatedAt': now, 'engine': engine_health(store, now), 'settings': store.settings(),
            'agents': [{**a, 'openWork': open_load.get(a['id'], 0)} for a in store.agents()],
            'counts': counts, 'notYetQueued': waiting, 'handoffs': handoffs,
            'work': [w for w in work if not w['closed']]}


def cmd_status(args):
    store = open_store(args)
    report = status_report(store)
    now = report['generatedAt']
    engine, settings = report['engine'], report['settings']
    lines = ['Handoffs database: ' + report['db']]
    if engine['running']:
        lines.append('Engine: running (last pass ' + ago(engine['lastPassAt'], now) + ')')
    elif engine['lastPassAt']:
        lines.append('Engine: not running (last pass ' + ago(engine['lastPassAt'], now) + '). Start it with "handoffs serve".')
    else:
        lines.append('Engine: never ran. Start it with "handoffs serve" or "handoffs run".')
    lines.append('Delivery: ' + ('on' if settings['enabled'] else 'paused') + ' · connections: ' + settings['connections']
                 + ' · outage guard: ' + ('on' if settings['outageGuard'] else 'off'))
    lines += ['', 'Agents']
    if report['agents']:
        lines.append(table([[a['id'], a['name'], a['provider'], a['openWork']] for a in report['agents']],
                           ['ID', 'NAME', 'PROVIDER', 'OPEN WORK']))
    else:
        lines.append('  none yet (handoffs agent add ID --provider demo)')
    counts = report['counts']
    in_flight = sum(n for s, n in counts.items() if s not in TERMINAL and s not in ATTENTION)
    attention = [h for h in report['handoffs'] if h['needsAttention']]
    finished = sum(n for s, n in counts.items() if s in TERMINAL)
    lines += ['', 'Handoffs: ' + str(in_flight) + ' in flight · ' + str(len(attention)) + ' need attention · '
              + str(finished) + ' finished'
              + (' · ' + str(report['notYetQueued']) + ' new, picked up on the next engine pass'
                 if report['notYetQueued'] else '')]

    def rows(items):
        return table([[h['id'][:8], clip(h['title'], 32), clip(h['senderName'], 14) + ' → ' + clip(h['recipientName'], 14),
                       h['status'], ago(h['updated'], now), clip(h['detail'], 60)] for h in items],
                     ['ID', 'TITLE', 'FROM → TO', 'STATE', 'CHANGED', 'DETAIL'])
    if attention:
        lines += ['Needs attention', rows(attention[:10])]
        if any(h['status'] in ACTIVE for h in attention):
            lines.append('  A stuck delivery holds its recipient. If you know how it ended: handoffs release ID')
    latest = [h for h in report['handoffs'] if not h['needsAttention']][:10]
    if latest:
        lines += ['Latest', rows(latest)]
    if report['work']:
        lines += ['', 'Open work']
        lines.append(table([[w['message_id'][:8], clip(w['title'], 40),
                             clip(store.name(w['sender']), 14) + ' → ' + clip(store.name(w['recipient']), 14), w['state']]
                            for w in report['work'][:20]], ['ID', 'TITLE', 'FROM → TO', 'STATE']))
    out(args, report, '\n'.join(lines))
    return 0


def cmd_tick(args):
    store = open_store(args)
    # One pass, then exit: turns that would live inside this process (Codex, Claude Code)
    # are left for a long-running engine instead of being started and cut off.
    engine = build_engine(store, one_shot=True)
    try:
        report = engine.tick()
    finally:
        engine.close()
    if report.get('skipped'):
        out(args, report, 'Skipped: ' + report['skipped'] + '.')
        return 0
    text = ('Checked ' + str(report['checked']) + ', enrolled ' + str(report['enrolled']) + ', resends '
            + str(report['resends']) + ', overdue alerts ' + str(report['overdueAlerts']) + '.')
    for e in report['errors']:
        text += '\nError on ' + e['id'] + ': ' + e['error']
    out(args, report, text)
    return 0 if report['ok'] else 1


def cmd_release(args):
    """Stop tracking one unfinished delivery (for example an UNCERTAIN row nobody can observe)."""
    store = open_store(args)
    hid = resolve_message(store, args.id)
    before = store.handoff(hid)
    if before is None:
        raise ValueError('Message ' + hid + ' has no delivery to release (it was never queued for a turn).')
    row = store.release(hid, args.note or '')
    out(args, row, 'Released ' + hid + ' (it was ' + before['status'] + '). Nothing was sent or resent; later '
        'handoffs to ' + store.name(row['recipient']) + ' go ahead once it is idle.')
    return 0


def cmd_run(args):
    store = open_store(args)
    engine = build_engine(store)
    print('Delivering handoffs from ' + str(store.path) + ' every ' + str(args.interval) + ' s. Press Ctrl+C to stop.',
          flush=True)
    try:
        engine.run_forever(interval=args.interval)
    except KeyboardInterrupt:
        print('Stopped.')
    return 0


def page_url(host, port) -> str:
    shown = '127.0.0.1' if host in ('0.0.0.0', '', '::') else host
    return 'http://' + ('[' + shown + ']' if ':' in shown else shown) + ':' + str(port) + '/'


def cmd_serve(args):
    if args.public_demo:
        # A public page has no login. It is only for the throwaway database of `handoffs demo`,
        # never for a real one with real message text, work results and agent errors.
        raise UsageError('--public-demo is only for "handoffs demo" (simulated agents on a temporary database). '
                         '"handoffs serve" shows your real handoffs, so it stays on this computer.')
    store = open_store(args)
    from .web import serve
    engine = build_engine(store)
    print('Handoffs page: ' + page_url(args.host, args.port) + '  (delivery engine running; Ctrl+C to stop)', flush=True)
    try:
        serve_page(serve, store, engine, args)
    except KeyboardInterrupt:
        print('Stopped.')
    return 0


def serve_page(serve, store, engine, args):
    """Run the web page, turning a taken port into a plain message."""
    try:
        serve(store, engine=engine, host=args.host, port=args.port, public_demo=args.public_demo)
    except OSError as e:
        if e.errno in (errno.EADDRINUSE, errno.EACCES):
            raise ValueError('Cannot listen on ' + args.host + ':' + str(args.port) + ' (' + e.strerror
                             + '). Choose another port with --port.') from None
        raise


def cmd_mcp(args):
    store = open_store(args)
    require_agent(store, args.agent)
    from .mcp import open_context_library, serve_mcp
    library = open_context_library(args.context_library or os.environ.get('CONTEXTLIB_ROOT'))
    return serve_mcp(store, args.agent, context_library=library) or 0  # stdout belongs to the MCP protocol from here on


# ---- the demo ----------------------------------------------------------------------

DEMO_AGENTS = (('planner', 'Planner'), ('engineer', 'Engineer'), ('reviewer', 'Reviewer'), ('writer', 'Writer'))

# (sender, recipient, title, message, due minutes). Rotated forever by the demo feeder.
DEMO_WORK = (
    ('planner', 'engineer', 'Add retries to the upload client',
     'Uploads fail on flaky networks. Add bounded retries with backoff and a test that proves a retry happens.', 15),
    ('planner', 'writer', 'Draft the 0.2 release notes',
     'Summarize what changed since 0.1 for users. Keep it under 200 words and mention the new config flag.', 20),
    ('engineer', 'reviewer', 'Review the retry patch',
     'Please review the retry change: check the backoff cap and that the new test fails without the fix.', 10),
    ('writer', 'reviewer', 'Check the quick start',
     'Walk through the README quick start on a clean machine and note every step that is unclear.', 10),
    ('reviewer', 'engineer', 'Fix the flaky clock test',
     'The deadline test fails about once in fifty runs. Make the clock injectable and remove the sleep.', 15),
    ('planner', 'reviewer', 'Triage new bug reports',
     'Sort this week\'s bug reports into fix now, fix later and needs more information.', 20),
    ('engineer', 'writer', 'Document the timeout setting',
     'The command adapter now has a timeout setting. Add it to the adapter docs with one example.', 15),
    ('planner', 'engineer', 'Speed up the status query',
     'The status page query takes 300 ms with 10k rows. Find the slow part and fix it.', 20),
    ('reviewer', 'writer', 'Tighten the error messages',
     'Rewrite the five most common CLI errors so each says what went wrong and what to do next.', 15),
    ('writer', 'planner', 'Choose the next tutorial topic',
     'Pick one of: MCP setup, custom adapters or deadlines, and say why in two sentences.', 10),
)
DEMO_NOTES = (
    ('reviewer', 'planner', 'CI is green on main again after the clock fix.'),
    ('engineer', 'planner', 'Heads up: I am pairing with Reviewer on the retry patch this afternoon.'),
)


def _step_clock(store):
    """Give the next write a later timestamp when the store clock is a frozen test double."""
    at = getattr(store.clock, 'at', None)
    if isinstance(at, (int, float)):
        store.clock.at = at + 0.001


def seed_demo(store: Store, first=3):
    """Four simulated agents and a first few assignments so the page has life at once."""
    for agent_id, name in DEMO_AGENTS:
        store.register_agent(agent_id, name=name, provider='demo', endpoint='demo:' + agent_id)
        _step_clock(store)
    for sender, recipient, title, body, due in DEMO_WORK[:first]:
        store.send(sender, recipient, body, title=title, due_seconds=due * 60)
    sender, recipient, note = DEMO_NOTES[0]
    store.send(sender, recipient, note)


class DemoFeeder:
    """Plays the humans around the simulated agents so the demo keeps moving.

    Each pass it closes returned work the way a sender would (after the return card
    reached them), tops up new assignments while few are open, and prunes old
    finished rows so a long-running public demo stays small.
    """

    def __init__(self, store: Store, speed=1.0, clock=time.time, max_open=4, gap=12.0, keep_seconds=7200):
        self.store = store
        self.speed = max(float(speed), 0.1)
        self.clock = clock
        self.max_open = max_open
        self.gap = gap / self.speed
        self.keep_seconds = keep_seconds
        self.next_work = 3  # seed_demo already sent the first three
        self.last_sent = clock()
        self.passes = 0

    def step(self) -> dict:
        self.passes += 1
        report = {'closed': self.close_returned(), 'sent': self.top_up(), 'pruned': 0}
        if self.passes % 50 == 0:
            report['pruned'] = self.prune()
        return report

    @staticmethod
    def _pick(key, choices):
        """Stable pseudo-random choice, so a demo run is reproducible."""
        return choices[int(hashlib.sha256(key.encode()).hexdigest(), 16) % len(choices)]

    def close_returned(self) -> int:
        now, closed = self.clock(), 0
        for w in self.store.work_list(200):
            if w['closed']:
                continue
            if w['returned']:
                delivery = self.store.handoff(w['return_message_id']) if w['return_message_id'] else None
                seen = delivery is not None and delivery['status'] in TERMINAL
                if not seen and now - w['returned'] < 60 / self.speed:
                    continue  # let the return card reach the sender first
                if (w['result'] or {}).get('disposition') == 'BLOCKED':
                    outcome, note = 'blocked', 'Parking this until the blocker is cleared.'
                else:
                    outcome, note = self._pick(w['message_id'], (('accepted', 'Looks good, thanks.'),) * 5
                                               + (('revision', 'Close. Please tighten it and send it again.'),))
            elif now - w['created'] > 25 * 60:
                outcome, note = 'blocked', 'No result after 25 minutes; closing it in the demo.'
            else:
                continue
            self.store.close_work(w['message_id'], w['sender'], outcome, note)
            closed += 1
        return closed

    def top_up(self) -> int:
        now = self.clock()
        if now - self.last_sent < self.gap:
            return 0
        open_titles = {w['title'] for w in self.store.work_list(200) if not w['returned'] and not w['closed']}
        if len(open_titles) >= self.max_open:
            return 0
        for _ in range(len(DEMO_WORK)):
            sender, recipient, title, body, due = DEMO_WORK[self.next_work % len(DEMO_WORK)]
            self.next_work += 1
            if title not in open_titles:
                self.store.send(sender, recipient, body, title=title, due_seconds=due * 60)
                if self.next_work % 7 == 0:
                    note = DEMO_NOTES[self.next_work // 7 % len(DEMO_NOTES)]
                    self.store.send(note[0], note[1], note[2])
                self.last_sent = now
                return 1
        return 0

    def prune(self) -> int:
        """Delete finished demo history older than keep_seconds. Demo databases only."""
        cutoff = self.clock() - self.keep_seconds
        with self.store.connect() as db:
            done = [r['id'] for r in db.execute(
                'SELECT h.id FROM handoffs h LEFT JOIN work w ON w.message_id=h.id WHERE h.updated<? AND h.status IN '
                '(' + ','.join('?' * len(TERMINAL)) + ') AND (w.message_id IS NULL OR w.closed IS NOT NULL)',
                (cutoff, *TERMINAL))]
            for hid in done:
                if db.execute('SELECT 1 FROM work WHERE return_message_id=? AND closed IS NULL', (hid,)).fetchone():
                    continue
                db.execute('DELETE FROM events WHERE handoff_id=?', (hid,))
                db.execute('DELETE FROM handoffs WHERE id=?', (hid,))
                db.execute('DELETE FROM work WHERE message_id=?', (hid,))
                db.execute('DELETE FROM messages WHERE id=?', (hid,))
            db.execute('DELETE FROM messages WHERE intent=? AND created<?', ('notification', cutoff))
        return len(done)

    def run(self, stop: threading.Event, interval=1.0):
        while not stop.is_set():
            try:
                self.step()
            except Exception:  # noqa: BLE001 (a busy database or a race with the engine: try next pass)
                pass
            stop.wait(interval)


def cmd_demo(args):
    if args.speed <= 0:
        raise UsageError('--speed must be greater than 0.')
    from .web import serve
    workdir = tempfile.mkdtemp(prefix='handoffs-demo-')
    stop = threading.Event()
    feeder = None
    try:
        store = Store(os.path.join(workdir, 'demo.sqlite3'))
        seed_demo(store)
        engine = build_engine(store, speed=args.speed)
        feeder = threading.Thread(target=DemoFeeder(store, speed=args.speed).run, args=(stop,), daemon=True,
                                  name='handoffs-demo-feeder')
        feeder.start()
        print('\n'.join([
            'AgentBrain Handoffs demo',
            '  Open ' + page_url(args.host, args.port),
            '  Four simulated agents (Planner, Engineer, Reviewer, Writer) hand work to each other.',
            '  Nothing real is contacted. The demo data is temporary and deleted when you stop (Ctrl+C).',
        ]), flush=True)
        if args.public_demo:
            print('Public demo mode: any Host header is served and sending from the page is off.', flush=True)
        serve_page(serve, store, engine, args)
    except KeyboardInterrupt:
        print('Demo stopped.')
    finally:
        stop.set()
        if feeder is not None:
            feeder.join(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)
    return 0


# ---- the parser --------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--db', default=argparse.SUPPRESS, metavar='PATH',
                        help='the handoff database (default: $HANDOFFS_DB, else ./.handoffs/handoffs.sqlite3)')
    as_json = argparse.ArgumentParser(add_help=False)
    as_json.add_argument('--json', action='store_true', help='print JSON instead of text')
    as_agent = argparse.ArgumentParser(add_help=False)
    as_agent.add_argument('--as', dest='actor', metavar='AGENT', help='the agent you act as (default: $HANDOFFS_AGENT)')

    parser = argparse.ArgumentParser(
        prog='handoffs', description='Exact, durable handoffs between AI coding agents.',
        epilog='Start with "handoffs demo" to watch simulated agents, or "handoffs init" to set up your own.')
    parser.add_argument('--db', default=None, metavar='PATH',
                        help='the handoff database (default: $HANDOFFS_DB, else ./.handoffs/handoffs.sqlite3)')
    parser.add_argument('--version', action='version', version='handoffs ' + __version__)
    sub = parser.add_subparsers(dest='command', metavar='COMMAND')

    def command(name, func, help_text, parents=()):
        p = sub.add_parser(name, help=help_text, description=help_text, parents=[common, *parents])
        p.set_defaults(func=func)
        return p

    command('init', cmd_init, 'create the handoff database', [as_json])

    agent = sub.add_parser('agent', help='add, list or remove agents', description='Add, list or remove agents.')
    agent_sub = agent.add_subparsers(dest='agent_command', metavar='ACTION')
    agent.set_defaults(func=cmd_agent_missing)
    p = agent_sub.add_parser('add', parents=[common, as_json], help='register an agent or update one',
                             description='Register an agent, or update the given fields of an existing one.')
    p.add_argument('id', help='a stable id, for example planner or codex-main')
    p.add_argument('--name', help='the display name (default: the id)')
    p.add_argument('--provider', choices=PROVIDERS, help='which adapter delivers to it (required for a new agent)')
    p.add_argument('--endpoint', help='the existing session to deliver into (codex thread id, claude session id)')
    p.add_argument('--cwd', help='the project folder the agent works in')
    p.add_argument('--set', action='append', metavar='KEY=VALUE', help='an adapter setting; VALUE may be JSON (repeatable)')
    p.set_defaults(func=cmd_agent_add)
    p = agent_sub.add_parser('list', parents=[common, as_json], help='list agents')
    p.set_defaults(func=cmd_agent_list)
    p = agent_sub.add_parser('remove', parents=[common, as_json], help='remove an agent')
    p.add_argument('id')
    p.set_defaults(func=cmd_agent_remove)

    for name, func, text in (('allow', cmd_allow, 'allow SENDER to hand off to RECIPIENT'),
                             ('block', cmd_block, 'block SENDER from handing off to RECIPIENT')):
        p = command(name, func, text, [as_json])
        p.add_argument('sender')
        p.add_argument('recipient')

    p = command('config', cmd_config, 'show or change a setting: enabled, enabledAfter, connections, outageGuard', [as_json])
    p.add_argument('key', nargs='?')
    p.add_argument('value', nargs='?', help='a JSON value; plain words are taken as text; enabledAfter accepts "now"')

    p = command('send', cmd_send, 'send a message; with --title it becomes a work contract', [as_json])
    p.add_argument('sender')
    p.add_argument('recipient')
    p.add_argument('message', help='the message text, or - to read it from stdin')
    p.add_argument('--title', help='make it work with this title; the recipient must return a result')
    p.add_argument('--due-minutes', type=int, metavar='N', help='deadline for the work, in minutes')
    p.add_argument('--key', help='an idempotency key: sending again with the same key does nothing')

    p = command('inbox', cmd_inbox, "show an agent's unread messages", [as_json])
    p.add_argument('agent')
    p.add_argument('--all', action='store_true', help='include messages already read')
    p.add_argument('--limit', type=int, default=50)

    p = command('read', cmd_read, 'show a message; marks it read when you are the recipient', [as_json, as_agent])
    p.add_argument('id', help='message id, or a unique prefix of at least 6 characters')

    p = command('accept', cmd_accept, 'accept assigned work (optional; returning implies it)', [as_json, as_agent])
    p.add_argument('id')

    p = command('return', cmd_return, 'return a result for work assigned to you', [as_json, as_agent])
    p.add_argument('id')
    p.add_argument('--summary', required=True, help='one line: what you did or what blocks you')
    p.add_argument('--evidence', help='where to verify it: a commit, a test run, a file')
    p.add_argument('--blocked', action='store_true', help='report a blocker instead of a finished result')

    p = command('close', cmd_close, 'record your decision on work you assigned', [as_json, as_agent])
    p.add_argument('id')
    p.add_argument('outcome', choices=CLOSE_OUTCOMES)
    p.add_argument('--note', help='a short reason, shown to the assignee')

    command('status', cmd_status, 'agents, deliveries, open work and engine health', [as_json])
    command('tick', cmd_tick, 'run one delivery pass and exit (Codex and Claude Code turns need run or serve)',
            [as_json])

    p = command('release', cmd_release, 'stop tracking a stuck delivery; it becomes CANCELLED and is never resent',
                [as_json])
    p.add_argument('id', help='message id, or a unique prefix of at least 6 characters')
    p.add_argument('--note', help='why, shown on the delivery')

    p = command('run', cmd_run, 'run the delivery engine in the foreground')
    p.add_argument('--interval', type=float, default=5.0, help='seconds between passes (default 5)')

    p = command('serve', cmd_serve, 'run the delivery engine and the live web page')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8765)
    p.add_argument('--public-demo', action='store_true', help=argparse.SUPPRESS)  # refused: demo only

    p = command('mcp', cmd_mcp, 'run an MCP server (stdio) for one agent')
    p.add_argument('--agent', required=True, help='the agent this MCP server acts as; fixed for its lifetime')
    p.add_argument('--context-library', help="also serve this ContextLib library's context_* tools "
                                             '(default: $CONTEXTLIB_ROOT when set; needs agentbrain-contextlib)')

    p = sub.add_parser('demo', help='watch simulated agents hand off work on a live page',
                       description='Watch four simulated agents hand off work on a live page. Uses a temporary database.')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8765)
    p.add_argument('--speed', type=float, default=1.0, help='simulated agents work this many times faster (default 1)')
    p.add_argument('--public-demo', action='store_true', help='serve any Host header and turn off sending from the page')
    p.set_defaults(func=cmd_demo)
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:  # argparse: 2 for usage errors, 0 for --help and --version
        return e.code if isinstance(e.code, int) else 2
    if not getattr(args, 'func', None):
        parser.print_help(sys.stderr)
        return 2
    try:
        return args.func(args) or 0
    except UsageError as e:
        print('handoffs: ' + str(e), file=sys.stderr)
        return 2
    except ImportError as e:
        print('handoffs: this command needs a part of the package that failed to load: ' + str(e), file=sys.stderr)
        return 1
    except (ValueError, OSError, sqlite3.Error) as e:
        print('handoffs: ' + str(e), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
