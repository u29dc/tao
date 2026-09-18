import sqlite3
import tempfile
import unittest
from pathlib import Path

import release

import fixtures


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='fixture-test-', dir=release.ROOT / 'target')
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_metadata_and_manifest_are_deterministic(self):
        root = fixtures.generate(self.root, 'custom', 30, 42, 'tiny')
        first = fixtures.validate(root)
        project = (root / 'notes/projects/project-1.md').read_text().split('---\n')[1]
        self.assertIn('status:', project)
        self.assertIn('priority:', project)
        self.assertGreater(first['property_counts']['aliases'], 0)
        fixtures.generate(self.root, 'custom', 30, 42, 'tiny')
        self.assertEqual(first, fixtures.validate(root))
        (root / 'notes/projects/project-1.md').write_text('# unexpected edit')
        with self.assertRaises(ValueError):
            fixtures.validate(root)

    def test_missing_ingested_metadata_is_rejected(self):
        root = fixtures.generate(self.root, 'custom', 30, 42, 'tiny')
        db = self.root / 'index.sqlite'
        connection = sqlite3.connect(db)
        connection.executescript(
            'CREATE TABLE files(file_id TEXT, normalized_path TEXT, is_markdown INTEGER); CREATE TABLE properties(file_id TEXT,key TEXT,value_json TEXT);'
        )
        connection.commit()
        connection.close()
        with self.assertRaisesRegex(ValueError, 'indexed project semantics'):
            fixtures.validate(root, db)

    def test_representative_fixture_contains_varied_sizes_and_content(self):
        root = fixtures.generate(self.root, 'custom', 30, 42, 'representative')
        manifest = fixtures.validate(root)
        self.assertGreater(manifest['markdown_bytes']['max'], 700000)
        self.assertIn('.pdf', manifest['files_by_extension'])
        self.assertIn('.txt', manifest['files_by_extension'])
        self.assertIn('.bin', manifest['files_by_extension'])

    def test_parity_copy_preserves_golden_and_attachment(self):
        fixtures.parity(self.root)
        self.assertTrue((self.root / 'graph/vault/assets/lone.pdf').is_file())
        self.assertTrue((self.root / 'graph/expected/links-outgoing.json').is_file())
        with self.assertRaises(ValueError):
            fixtures.parity(release.ROOT / 'fixtures')

    def test_refuses_unmanaged_and_symlink_generation(self):
        output = self.root / 'vault-custom'
        output.mkdir()
        (output / 'sentinel').write_text('keep')
        with self.assertRaises(ValueError):
            fixtures.generate(self.root, 'custom', 30, 42, 'tiny')
        self.assertEqual((output / 'sentinel').read_text(), 'keep')
        (self.root / 'link').symlink_to(output, target_is_directory=True)
        with self.assertRaises(ValueError):
            fixtures.generate(self.root / 'link', 'custom', 30, 42, 'tiny')

    def test_manifest_rejects_directory_and_dangling_symlinks(self):
        root = fixtures.generate(self.root, 'custom', 30, 42, 'tiny')
        link = root / 'unexpected-link'
        for destination in (self.root, self.root / 'absent'):
            link.symlink_to(destination)
            with self.assertRaises(ValueError):
                fixtures.validate(root)
            link.unlink()
