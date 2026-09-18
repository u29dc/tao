"""Filesystem boundaries shared by release, fixtures and benchmarks."""

import tempfile
import unittest
from pathlib import Path

import support


class PathTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=support.ROOT / 'target')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_repository_output_accepts_missing_nested_destinations(self):
        destination = self.root / 'missing/nested'
        self.assertEqual(support.repo_output(destination), destination)

    def test_repository_output_rejects_external_and_sibling_prefixes(self):
        for destination in (Path('/tmp'), Path(str(support.ROOT) + '-other/nested')):
            with self.subTest(destination=destination), self.assertRaises(ValueError):
                support.repo_output(destination)

    def test_repository_output_rejects_symlink_escapes(self):
        link = self.root / 'escape'
        link.symlink_to('/tmp', target_is_directory=True)
        with self.assertRaises(ValueError):
            support.repo_output(link / 'nested')

    def test_live_vault_requires_existing_non_broad_directory(self):
        self.assertEqual(support.existing_vault(self.root), self.root)
        for destination in (self.root / 'missing', Path('/'), Path.home()):
            with self.subTest(destination=destination), self.assertRaises(ValueError):
                support.existing_vault(destination)

    def test_output_rejects_broad_repository_and_home_destinations(self):
        for destination in (support.ROOT, support.ROOT.parent, Path('/'), Path.home()):
            with self.subTest(destination=destination), self.assertRaises(ValueError):
                support.canonical_destination(destination)
