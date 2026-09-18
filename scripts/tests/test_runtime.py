import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release
import support
import test


class RuntimeTests(unittest.TestCase):
    def test_bun_separator_before_options_is_supported(self):
        for argv in (['tool', '--', '--help'], ['tool', 'install', '--', '--out', 'somewhere']):
            with patch.object(sys, 'argv', argv):
                self.assertEqual(
                    support.script_arguments(('install',)), [arg for arg in argv[1:] if arg != '--']
                )

    def test_test_never_inherits_application_configuration(self):
        with patch.dict(
            'os.environ',
            {
                'TAO_CONFIG_PATH': '/private/global.toml',
                'TAO_DB_PATH': '/private/index.sqlite',
                'TAO_FEATURE_FLAGS': 'legacy',
                'DEVELOPER_DIR': '/compiler/path',
            },
        ):
            env = test.test_environment('/repository/target/qa')
        self.assertEqual(env['TAO_CONFIG_PATH'], '/repository/target/qa/absent-global-config.toml')
        self.assertEqual(env['TMPDIR'], '/repository/target/qa')
        self.assertEqual(env['DEVELOPER_DIR'], '/compiler/path')
        self.assertEqual({key for key in env if key.startswith('TAO_')}, {'TAO_CONFIG_PATH'})


class CargoWrapperTests(unittest.TestCase):
    def test_toolchain_selection_preserves_override_arguments_and_exit_status(self):
        with tempfile.TemporaryDirectory(
            prefix='cargo-wrapper-', dir=release.ROOT / 'target'
        ) as raw:
            directory = Path(raw)
            fake_cargo = directory / 'cargo'
            fake_cargo.write_text(
                '#!/bin/sh\nprintf "%s\\n" "${DEVELOPER_DIR-<unset>}" "$@"\nexit 23\n'
            )
            fake_cargo.chmod(0o755)
            fake_uname = directory / 'uname'
            standalone = '/Library/Developer/CommandLineTools'
            installed = os.access(Path(standalone) / 'usr/bin/clang', os.X_OK)
            for platform, configured, expected in (
                ('Linux', None, '<unset>'),
                ('Darwin', '/chosen toolchain', '/chosen toolchain'),
                ('Darwin', None, standalone if installed else '<unset>'),
                ('Darwin', '', standalone if installed else ''),
            ):
                with self.subTest(platform=platform, configured=configured):
                    fake_uname.write_text(f'#!/bin/sh\nprintf "%s\\n" "{platform}"\n')
                    fake_uname.chmod(0o755)
                    env = {
                        key: value for key, value in os.environ.items() if key != 'DEVELOPER_DIR'
                    }
                    env['PATH'] = str(directory) + os.pathsep + env.get('PATH', '')
                    if configured is not None:
                        env['DEVELOPER_DIR'] = configured
                    arguments = ['test', '--target-dir', 'space dir', '--', 'literal;$(nothing)']
                    result = subprocess.run(
                        [str(release.CARGO), *arguments],
                        cwd=directory,
                        env=env,
                        text=True,
                        capture_output=True,
                    )
                    self.assertEqual(result.returncode, 23, result.stderr)
                    self.assertEqual(result.stdout.splitlines(), [expected, *arguments])
