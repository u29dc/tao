# Conformance Fixture Vault

Deterministic fixture vault used by QA integration tests.

## Coverage Map

- Link resolution:
  - same-folder vs cross-folder ambiguity (`notes/apple.md` vs `docs/apple.md`)
  - unresolved wikilinks (`[[missing-note]]`)
  - heading fragments (`[[beta#Target Heading]]`)
  - block fragments (`[[beta#^beta-block]]`)
- Property ingestion:
  - scalars (`status`, `priority`, `due`)
  - lists (`tags`)
  - alias normalization (`aliases`)
- Malformed front matter tolerance:
  - broken YAML in `notes/malformed-frontmatter.md`
- Bases parsing and table execution:
  - valid table config in `views/projects.base`
  - invalid base config in `views/invalid.base`

## Notes

- Paths are lower-case and stable for deterministic snapshots.
- Asset files are included so scanners/indexers exercise non-markdown handling.
