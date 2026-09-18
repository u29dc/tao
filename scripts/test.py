#!/usr/bin/env python3
"""Run tooling suites or isolated Rust tests with repository-local temporary state."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from support import CARGO, ROOT, repo_output, script_arguments


def test_environment(temporary):
    env = {key: value for key, value in os.environ.items() if not key.startswith('TAO_')}
    env['TAO_CONFIG_PATH'] = str(Path(temporary) / 'absent-global-config.toml')
    env['TMPDIR'] = str(temporary)
    return env


def main():
    target = repo_output(ROOT / 'target')
    target.mkdir(exist_ok=True)
    args = script_arguments(('rust', 'scripts'))
    if args[:1] == ['scripts']:
        loader = unittest.TestLoader()
        suite = (
            loader.loadTestsFromNames([f'tests.test_{name}' for name in args[1:]])
            if len(args) > 1
            else loader.discover(str(ROOT / 'scripts/tests'), top_level_dir=str(ROOT / 'scripts'))
        )
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if args[:1] == ['rust']:
        args = args[1:]
    with tempfile.TemporaryDirectory(prefix='tao-tests-', dir=target) as temporary:
        # Stop upward config discovery inside the disposable test boundary.
        # An edited repository config must never redirect fixture state elsewhere.
        (Path(temporary) / '.git').mkdir()
        result = subprocess.run(
            [str(CARGO), 'test', '--workspace', '--release', '--target-dir', str(target), *args],
            cwd=ROOT,
            env=test_environment(temporary),
        )
    return result.returncode


if __name__ == '__main__':
    sys.exit(main())
