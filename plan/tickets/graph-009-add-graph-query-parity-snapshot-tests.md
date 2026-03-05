## GRAPH-009 Add graph query parity snapshot tests

Status: done

### Objective
Lock graph output behavior using golden snapshots across representative fixtures.

### Scope
Integration tests and deterministic fixture baselines.

### Concrete Steps
1. Create snapshot fixtures for outgoing, backlinks, unresolved, deadends, orphans, and walk.
2. Generate expected JSON outputs and commit as goldens.
3. Add test runner to compare live output to goldens.
4. Document snapshot update process in test comments.

### Required Files and Locations
- crates/tao-cli/tests/graph_snapshots.rs
- vault/generated
- plan/checklists/review.md

### Implementation Notes
Snapshots must be stable by sorting keys and arrays.

### Dependencies
- GRAPH-001
- GRAPH-005

### Acceptance Criteria
- [ ] Graph snapshot tests pass in repeated runs with same seed.
- [ ] Any output drift creates a clear diff in test failure.
- [ ] Coverage includes frontmatter-only link cases.
