#!/usr/bin/env python3
"""Kill check: run the named test modules; print CHECK_PASS:<names> only if all pass.

Usage: python3 tools/check.py engine store web ...   (maps to tests/test_<name>.py)
       python3 tools/check.py package                (fresh venv install + CLI smoke)
"""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def package_ok():
    with tempfile.TemporaryDirectory() as tmp:
        venv = Path(tmp) / 'v'
        subprocess.run([sys.executable, '-m', 'venv', str(venv)], check=True)
        pip = venv / 'bin' / 'pip'
        subprocess.run([str(pip), 'install', '-q', str(ROOT)], check=True)
        exe = venv / 'bin' / 'handoffs'
        for args in (['--help'], ['init', '--help'], ['demo', '--help'], ['mcp', '--help'], ['serve', '--help']):
            subprocess.run([str(exe), *args], check=True, stdout=subprocess.DEVNULL)
        db = Path(tmp) / 'h.sqlite3'
        env = dict(os.environ, HANDOFFS_DB=str(db))
        subprocess.run([str(exe), 'init'], check=True, env=env, stdout=subprocess.DEVNULL)
        subprocess.run([str(exe), 'agent', 'add', 'a', '--provider', 'demo'], check=True, env=env, stdout=subprocess.DEVNULL)
        subprocess.run([str(exe), 'agent', 'add', 'b', '--provider', 'demo'], check=True, env=env, stdout=subprocess.DEVNULL)
        subprocess.run([str(exe), 'send', 'a', 'b', 'hello'], check=True, env=env, stdout=subprocess.DEVNULL)
    return True


def main(names):
    ok = True
    modules = [n for n in names if n != 'package']
    if modules:
        sys.path.insert(0, str(ROOT / 'src'))
        sys.path.insert(0, str(ROOT / 'tests'))
        suite = unittest.defaultTestLoader.loadTestsFromNames(['test_' + n for n in modules])
        result = unittest.TextTestRunner(verbosity=1).run(suite)
        ok = result.wasSuccessful() and result.testsRun > 0
    if ok and 'package' in names:
        try:
            ok = package_ok()
        except subprocess.CalledProcessError as e:
            print('package check failed:', e, file=sys.stderr)
            ok = False
    print('CHECK_PASS:' + ','.join(names) if ok else 'CHECK_FAIL:' + ','.join(names))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
