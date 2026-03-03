# Obsidian Compatibility Matrix (v1)

## Scope

This matrix defines compatibility commitments for v1.

## Matrix

| Area | Feature | v1 Target | Status Definition | Evidence |
| --- | --- | --- | --- | --- |
| Vault | Folder mapping | required | Open existing vault root and discover notes/assets | Integration fixture test |
| Files | `.md` notes | required | Parse, index, render | Parser + render tests |
| Files | `.base` definitions | required | Parse table view config | Bases parser tests |
| Files | `.canvas` | passthrough | Show as file node only | Manual QA |
| Links | `[[note]]` | required | Deterministic resolution by resolver policy | Resolver determinism tests |
| Links | `[[note#heading]]` | required | Heading target resolution | Heading resolver tests |
| Links | `[[note#^block]]` | required | Block target resolution | Block resolver tests |
| Links | `[[note|alias]]` display text | required | Preserve display text while targeting note | Link parse tests |
| Properties | YAML front matter | required | Extract and normalize typed properties | Property extraction tests |
| Properties | malformed YAML tolerance | required | No crash, parse error persisted | Negative fixture tests |
| Bases | table view | required | Metadata-driven row query and render | Bases table integration tests |
| Bases | non-table views | out of scope | Not implemented in v1 | Scope contract |
| Search | lexical search | required | Indexed query path returns ranked matches | Search integration tests |
| UI | file tree/list nav | required | Open note from tree/list | Swift UI smoke tests |
| CLI | JSON envelope contract | required | One JSON envelope object to stdout | CLI schema tests |

## Explicit Non-Goals

- Plugin parity with Obsidian.
- Graph view parity.
- Sync and collaboration parity.

## Compatibility Policy

- Favor deterministic behavior over undocumented parity assumptions.
- When ambiguous behavior exists in Obsidian, define local resolver rules and document deviation.
- Keep compatibility corpus fixtures versioned with expected outputs.
