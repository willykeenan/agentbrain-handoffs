"""tools/release_check.py: a ref is publishable only if its whole history is clean.

Each test builds a throwaway git repository; nothing touches this checkout's history.
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('release_check', ROOT / 'tools' / 'release_check.py')
release_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release_check)

CLEAN = ('Release Bot', 'bot@users.noreply.github.com')
PERSONAL = ('Dev', 'dev' + '@' + 'gmail.com')


@unittest.skipUnless(shutil.which('git'), 'git is not installed')
class ReleaseCheckTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = Path(tmp.name)
        self.git('init', '-q')

    def git(self, *args, who=CLEAN):
        env = dict(os.environ, GIT_AUTHOR_NAME=who[0], GIT_AUTHOR_EMAIL=who[1], GIT_COMMITTER_NAME=who[0],
                   GIT_COMMITTER_EMAIL=who[1], GIT_CONFIG_NOSYSTEM='1')
        subprocess.run(['git', '-C', str(self.repo), '-c', 'commit.gpgsign=false', *args], check=True, env=env,
                       capture_output=True)

    def commit(self, files, message='Add files', who=CLEAN):
        for name, text in files.items():
            (self.repo / name).write_text(text)
        self.git('add', '-A')
        self.git('commit', '-q', '-m', message, who=who)

    def test_a_clean_history_passes(self):
        self.commit({'README.md': 'A clean project.\n'})
        self.assertEqual(release_check.problems(self.repo, 'HEAD', {}), [])

    def test_a_personal_email_anywhere_in_history_fails(self):
        self.commit({'README.md': 'First.\n'}, who=PERSONAL)
        self.commit({'README.md': 'Second.\n'})
        found = release_check.problems(self.repo, 'HEAD', {})
        self.assertTrue(any('author email' in line for line in found), found)

    def test_a_private_name_in_an_old_version_of_a_file_fails(self):
        env = {'HANDOFFS_PRIVATE_PATTERNS': 'Internal Tool'}
        self.commit({'SPEC.md': 'Never mention Internal Tool.\n'})
        self.commit({'SPEC.md': 'Clean now.\n'})  # the tree is clean, the history is not
        found = release_check.problems(self.repo, 'HEAD', env)
        self.assertTrue(any('SPEC.md' in line for line in found), found)

    def test_a_private_name_in_a_commit_message_fails(self):
        self.commit({'README.md': 'Fine.\n'}, message='Import from the Internal Tool build')
        found = release_check.problems(self.repo, 'HEAD', {'HANDOFFS_PRIVATE_PATTERNS': 'Internal Tool'})
        self.assertTrue(any('commit message' in line for line in found), found)

    def test_a_fresh_orphan_commit_of_the_clean_tree_passes(self):
        env = {'HANDOFFS_PRIVATE_PATTERNS': 'Internal Tool'}
        self.commit({'SPEC.md': 'Never mention Internal Tool.\n'}, who=PERSONAL)
        self.commit({'SPEC.md': 'Clean now.\n'})
        self.assertNotEqual(release_check.problems(self.repo, 'HEAD', env), [])
        self.git('checkout', '-q', '--orphan', 'release')
        self.git('commit', '-q', '-m', 'Release')
        self.assertEqual(release_check.problems(self.repo, 'release', env), [])


if __name__ == '__main__':
    unittest.main()
