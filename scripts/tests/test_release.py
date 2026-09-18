import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import release
import support


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='release-test-', dir=release.ROOT / 'target')
        self.root = Path(self.temp.name)
        (self.root / 'target').mkdir()
        self.patch = patch.object(release, 'ROOT', self.root)
        self.patch.start()
        self.support_patch = patch.object(support, 'ROOT', self.root)
        self.support_patch.start()
        self.codesign = patch.object(release.shutil, 'which', return_value=None)
        self.codesign.start()

    def tearDown(self):
        self.codesign.stop()
        self.patch.stop()
        self.support_patch.stop()
        self.temp.cleanup()

    def executable(self, name='candidate', body='exit 0'):
        path = self.root / name
        path.write_text('#!/bin/sh\n' + body + '\n')
        path.chmod(0o755)
        return path

    def test_packaging_never_installs_and_checks_extracted_artifact(self):
        binary = self.executable(body='echo "tao test"')
        install = self.root / 'unrelated-install'
        install.mkdir()
        (install / 'tao').write_text('keep')
        with patch.dict('os.environ', {'TAO_HOME': str(install)}):
            release.package(binary, self.root / 'dist')
        self.assertEqual((install / 'tao').read_text(), 'keep')
        self.assertTrue((self.root / 'dist/tao-cli-bundle.tar.gz.sha256').exists())
        bad = self.executable('bad', 'exit 1')
        with self.assertRaises(Exception):
            release.package(bad, self.root / 'bad-dist')
        self.assertFalse((self.root / 'bad-dist/tao-cli-bundle.tar.gz').exists())

    def test_failed_installed_self_check_restores_previous(self):
        install = self.root / 'install'
        install.mkdir()
        (install / 'tao').write_text('old binary')
        (install / 'config.toml').write_text('preserve config')
        binary = self.executable(body='case "$0" in */.tao-install-*/*) exit 0;; *) exit 1;; esac')
        with self.assertRaises(Exception):
            release.install(binary, install)
        self.assertEqual((install / 'tao').read_text(), 'old binary')
        self.assertEqual((install / 'config.toml').read_text(), 'preserve config')

    def test_managed_uninstall_preserves_runtime_and_refuses_changed_binary(self):
        install = self.root / 'install'
        release.install(self.executable(), install)
        (install / 'config.toml').write_text('keep')
        original = (install / 'tao').read_bytes()
        (install / 'tao').write_text('changed')
        with self.assertRaises(ValueError):
            release.uninstall(install)
        (install / 'tao').write_bytes(original)
        release.uninstall(install)
        self.assertEqual((install / 'config.toml').read_text(), 'keep')
        self.assertFalse((install / 'tao').exists())

    def test_cleanup_refuses_symlink_and_never_uses_install_environment(self):
        external = self.root / 'install'
        external.mkdir()
        (external / 'keep').write_text('keep')
        (self.root / 'dist').symlink_to(external, target_is_directory=True)
        with patch.dict('os.environ', {'TAO_HOME': str(external)}), self.assertRaises(ValueError):
            release.clean(dry_run=True)
        self.assertEqual((external / 'keep').read_text(), 'keep')
        with self.assertRaises(ValueError):
            release.uninstall(self.root)
