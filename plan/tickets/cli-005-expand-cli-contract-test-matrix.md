## CLI-005 Expand CLI contract test matrix

Status: done

### Objective
Guarantee JSON contract stability for all read-only commands.

### Scope
CLI test suite only.

### Concrete Steps
1. Enumerate all read-only commands by group and create table-driven tests.
2. Validate envelope shape, command identifier, and key payload fields.
3. Add negative tests for invalid args, missing files, and invalid base views.
4. Ensure tests run against repository fixture/generator output only.

### Required Files and Locations
- crates/tao-cli/tests/json_contracts.rs
- crates/tao-cli/tests/help_snapshot.rs
- vault/

### Implementation Notes
Keep tests deterministic and avoid external environment dependencies.

### Dependencies
- CLI-002
- CLI-003
- CLI-004

### Acceptance Criteria
- [x] Contract tests cover every read-only command route.
- [x] Negative cases assert error code and message shape.
- [x] Test suite passes repeatedly with no order-dependent flakiness.
