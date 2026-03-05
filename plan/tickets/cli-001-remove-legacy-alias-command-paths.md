## CLI-001 Remove legacy alias command paths

Status: done

### Objective
Complete clean-cut command surface with no legacy alias implementation.

### Scope
CLI parser and dispatch only.

### Concrete Steps
1. Identify and remove any legacy top-level aliases (note, links, properties, bases, search) if still present.
2. Keep only vault, doc, base, graph, meta, task, query command groups.
3. Update help snapshots and JSON contract tests.
4. Update AGENTS contract section to remove alias references.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-cli/tests/help_snapshot.rs
- AGENTS.md

### Implementation Notes
No compatibility shim layer should remain.

### Dependencies
- none

### Acceptance Criteria
- [x] tao --help lists only final command surface groups.
- [x] Legacy aliases return unknown-command errors.
- [x] Contract tests pass with clean-cut surface.
