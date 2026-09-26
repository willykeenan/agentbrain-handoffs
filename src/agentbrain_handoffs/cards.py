"""Readable handoff cards and the exact delivery marker.

A delivered handoff is one user message: a short human card first, then a
technical block that starts with 'HANDOFF <request id>'. Observers find a delivery
only by that marker at that exact position, so quoted text can never impersonate it.
Presentation never grants authority or changes work state.
"""
from __future__ import annotations

import re
import unicodedata

CARD_MARK = '🤝 '
DETAILS = '\n\n---\nTechnical details (agent-only)\n'
CARD_PREFIXES = ('From ', 'Result: ', 'Summary: ', 'Still open: ', 'Next step: ', 'Decision: ', 'Due: ', 'Message:', '>')
BIDI = dict.fromkeys(map(ord, '‪‫‬‭‮⁦⁧⁨⁩'))


def plain(value, cap=180) -> str:
    """One line of inert text: no control characters, no Markdown/HTML/link syntax."""
    value = ''.join(c for c in str(value or '') if not unicodedata.category(c).startswith('C'))
    value = ' '.join(value.split())
    if len(value) > cap:
        value = value[:cap - 1] + '…'
    value = re.sub(r'(?i)\bwww\.', 'www．', value).replace('://', ':／／').replace('@', '＠')
    return value.translate(str.maketrans({'<': '‹', '>': '›', '[': '［', ']': '］', '`': 'ˋ', '*': '＊',
                                          '_': '＿', '~': '～', '\\': '＼'}))


def quote_block(value, cap=20000) -> str:
    """Quote text line by line so no quoted line can pose as a control line."""
    text = ''.join(c for c in str(value or '') if c in '\n\t' or unicodedata.category(c) != 'Cc').translate(BIDI)
    if len(text) > cap:
        text = text[:cap] + '\n… (truncated; the full text is the original inbox message)'
    text = text.replace('![', '!​[')  # never let quoted Markdown pull a remote image
    return '\n'.join('> ' + line if line.strip() else '>' for line in (text.splitlines() or ['(empty)']))


def subject(body) -> str:
    first = str(body or '').strip().splitlines()[0] if str(body or '').strip() else ''
    if not first or len(first) > 60 or re.search(r'\b[0-9a-f]{8}\b|/|\{|\}', first):
        return 'New assignment'
    return first


def card(sender_name, recipient_name, *, body='', kind='assignment', work=None, due_at=None) -> str:
    """kind: 'assignment' (new work or a message), 'return' (a result coming back), 'overdue'."""
    work = work or {}
    lines = []
    if kind == 'return':
        result = work.get('result') or {}
        blocked = result.get('disposition') == 'BLOCKED'
        closure = work.get('closure')
        status = 'Blocked' if blocked else 'Review needed'
        if closure:
            status = {'accepted': 'Accepted', 'revision': 'Changes needed', 'blocked': 'Blocked'}[closure['outcome']]
        lines += [CARD_MARK + '**' + plain(work.get('title') or 'Work update', 80) + '** — ' + status,
                  'From ' + plain(sender_name, 60) + ' → ' + plain(recipient_name, 60), '',
                  'Result: ' + ('The assigned agent reported a blocker.' if blocked else 'The assigned agent returned a result.')]
        if result.get('summary'):
            lines.append('Summary: ' + plain(result['summary'], 600))
        lines.append('Still open: ' + ('The decision is recorded.' if closure else 'The result has not been accepted yet.'))
        lines.append('Next step: ' + ('Follow the recorded decision.' if closure else 'Review the evidence and record a decision.'))
        if closure:
            lines.append('Decision: ' + closure['outcome'] + ('. Note: ' + plain(closure['note'], 300) if closure.get('note') else ''))
        return '\n'.join(lines)
    if kind == 'overdue':
        lines += [CARD_MARK + '**' + plain(work.get('title') or 'Work deadline', 80) + '** — Needs attention',
                  'From ' + plain(sender_name, 60) + ' → ' + plain(recipient_name, 60), '',
                  'Result: The deadline passed without a returned result.',
                  'Still open: This does not show whether the assigned agent finished.',
                  'Next step: Check on the work and resolve the blocker or record a decision.']
        return '\n'.join(lines)
    title = work.get('title') or subject(body)
    lines += [CARD_MARK + '**' + plain(title, 80) + '** — Action requested',
              'From ' + plain(sender_name, 60) + ' → ' + plain(recipient_name, 60)]
    if due_at:
        lines.append('Due: ' + str(due_at))
    lines += ['', 'Message:', quote_block(body)]
    return '\n'.join(lines)


def technical(request_id, sender, recipient, message_id, instructions='') -> str:
    return ('HANDOFF ' + request_id + '\nExact sender: ' + sender + '\nExact recipient: ' + recipient +
            '\nMessage: ' + message_id + ('\n\n' + instructions if instructions else ''))


def envelope(card_text, request_id, sender, recipient, message_id, instructions='') -> str:
    return card_text + DETAILS + technical(request_id, sender, recipient, message_id, instructions)


def _card_head(head) -> bool:
    lines = head.splitlines()
    return (len(head) <= 60000 and len(lines) >= 2 and lines[0].startswith(CARD_MARK) and lines[1].startswith('From ')
            and all(line == '' or line.startswith(CARD_PREFIXES) for line in lines[2:]))


def marker_matches(body, request_id) -> bool:
    """True only for a real delivery of this request: marker first, or right after a card."""
    if not isinstance(body, str) or not request_id:
        return False
    if body.startswith('HANDOFF ' + request_id + '\n'):
        return True
    head, separator, tail = body.partition(DETAILS)
    return bool(separator and _card_head(head) and tail.startswith('HANDOFF ' + request_id + '\nExact sender: '))
