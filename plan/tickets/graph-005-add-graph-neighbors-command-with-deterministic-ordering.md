## GRAPH-005 Add graph neighbors command with deterministic ordering

Status: done

### Objective
Provide one-hop graph neighborhood lookup for note exploration.

### Scope
CLI graph command + SDK service API.

### Concrete Steps
1. Implement SDK service API returning outgoing and incoming neighbors for a note path.
2. Expose command `graph neighbors` with limit/offset and direction controls.
3. Sort output by canonical path and relation type for deterministic results.
4. Add CLI contract tests and integration tests.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- crates/tao-cli/tests/json_contracts.rs

### Implementation Notes
Keep neighbor query read-only and compatible with daemon mode.

### Dependencies
- GRAPH-001

### Acceptance Criteria
- [x] graph neighbors returns both incoming and outgoing links for a valid note.
- [x] Results are deterministic across repeated runs with the same dataset.
- [x] CLI JSON contract tests include the new command.
