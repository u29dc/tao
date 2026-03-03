# v1 Scope Contract

## Goal

Deliver a macOS-native knowledge app with a Swift UI and pure Rust SDK core for vault-native workflows.

## In Scope

- Open and map an existing vault folder.
- Parse and render Markdown notes.
- Resolve Obsidian-style wiki links, heading links, and block references.
- Parse front matter and expose typed properties.
- Provide file tree/list navigation and note view.
- Provide Bases-compatible table view for `.base` definitions.
- Provide minimal agent-native CLI wrappers over SDK APIs.

## Out of Scope

- Sync (cloud or peer-to-peer).
- Plugin runtime.
- Theme marketplace/customization system.
- Multi-user collaboration.
- Cross-platform support beyond macOS.
- Vector database or embeddings.

## Non-Functional Constraints

- Rust SDK is source-of-truth for domain semantics.
- UI layers cannot reimplement parser/resolver logic.
- `unsafe_code = "forbid"` workspace policy.
- Deterministic ticket execution through `tickets.csv` contract.

## Acceptance Criteria

1. Open a real vault and browse notes via tree/list without crashes.
2. Wiki link navigation resolves deterministically and supports backlinks.
3. Front matter is parsed to typed properties with malformed YAML tolerance.
4. `.base` table view loads rows using metadata-derived queries.
5. CLI JSON commands return exactly one envelope object to stdout.
6. Phase 0-5 quality gates pass: format, lint, types, tests, build, audit.

## Exit Criteria for v1

- Phase 5 complete with `REL-006` shipped.
- All non-TUI tickets marked `done`.
- Performance budgets tracked and non-regressing in CI.
