## QUERY-007 Add query integration parity matrix tests

Status: todo

### Objective
Verify query semantics across all scopes with one parity matrix.

### Scope
Integration tests and fixture matrix.

### Concrete Steps
1. Define parity matrix covering docs, graph, base, meta, and task scopes.
2. Add matrix-driven integration tests for where/sort/select/limit/offset combinations.
3. Capture expected row counts and deterministic ordering.
4. Record failures with compact diagnostics for fast triage.

### Required Files and Locations
- crates/tao-sdk-service/tests/conformance_harness.rs
- crates/tao-cli/tests/query_matrix.rs
- vault/generated

### Implementation Notes
Matrix tests should be deterministic with fixed fixture seed.

### Dependencies
- QUERY-002
- QUERY-003
- QUERY-004
- QUERY-005

### Acceptance Criteria
- [ ] All matrix cases pass on generated fixtures.
- [ ] Failures identify scope, operator, and expected/actual summary.
- [ ] Matrix includes frontmatter-link and relation-backed cases.
