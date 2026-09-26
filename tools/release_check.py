#!/usr/bin/env python3
"""Check that a git ref is safe to publish: its whole history, not just the working tree.

Usage: python3 tools/release_check.py [REF]      (default: HEAD)

A push publishes every commit reachable from the ref, with its author, committer,
message and every version of every file. This check fails when any of them carries:

- an author or committer email that is not a no-reply address;
- a private pattern in a commit message or in any file of any reachable commit.

Private patterns are generic markers (a home folder, a personal mail domain) plus any
listed in HANDOFFS_PRIVATE_PATTERNS (one per line or comma-separated), which a
maintainer or CI sets, so private names never have to be written into the repository.

Publish from a ref that passes, for example a fresh orphan branch:
    git checkout --orphan release && git commit ... && python3 tools/release_check.py release
"""
from __future__ import annotations

import os
import subprocess
import sys

GENERIC_PATTERNS = ('/' + 'Users/', '/' + 'home/', '@' + 'gmail.com')


def patterns(environ=None) -> tuple:
    raw = (os.environ if environ is None else environ).get('HANDOFFS_PRIVATE_PATTERNS', '')
    return GENERIC_PATTERNS + tuple(p.strip() for p in raw.replace(',', '\n').splitlines() if p.strip())


def git(repo, *args) -> str:
    done = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True)
    if done.returncode not in (0, 1):  # 1 is git grep's "no match"
        raise RuntimeError('git ' + ' '.join(args) + ' failed: ' + done.stderr.strip())
    return done.stdout


def problems(repo='.', ref='HEAD', environ=None) -> list:
    """Every reason the ref is not safe to publish (empty when it is)."""
    found = []
    pats = patterns(environ)
    commits = git(repo, 'rev-list', ref).split()
    if not commits:
        return ['The ref ' + ref + ' has no commits.']
    for line in git(repo, 'log', '--format=%h%x00%ae%x00%ce', ref).splitlines():
        short, author, committer = line.split('\0')
        for role, email in (('author', author), ('committer', committer)):
            if 'noreply' not in email.lower():
                found.append(short + ': the ' + role + ' email is not a no-reply address')
    for commit in commits:
        message = git(repo, 'log', '-1', '--format=%B', commit)
        for pat in pats:
            if pat in message:
                found.append(commit[:7] + ': the commit message contains a private pattern')
        grep = ['grep', '-I', '-l', '-F']
        for pat in pats:
            grep += ['-e', pat]
        for hit in git(repo, *grep, commit, '--').splitlines():
            found.append(commit[:7] + ': ' + hit.split(':', 1)[-1] + ' contains a private pattern')
    return sorted(set(found))


def main(argv) -> int:
    ref = argv[0] if argv else 'HEAD'
    try:
        found = problems('.', ref)
    except RuntimeError as error:
        print('release check could not run: ' + str(error), file=sys.stderr)
        return 2
    for line in found:
        print(line)
    print(('RELEASE_CHECK_FAIL:' if found else 'RELEASE_CHECK_PASS:') + ref)
    return 1 if found else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
