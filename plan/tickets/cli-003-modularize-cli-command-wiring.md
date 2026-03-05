## CLI-003 Modularize CLI command wiring

Status: done

### Objective
Break monolithic CLI entrypoint into maintainable command modules.

### Scope
CLI source organization only.

### Concrete Steps
1. Create modules per command family (vault/doc/base/graph/meta/task/query).
2. Extract shared argument parsing helpers and envelope writers.
3. Keep main.rs as thin bootstrap and dispatch router.
4. Update tests/imports with new module paths.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-cli/src/commands/
- crates/tao-cli/src/json_envelope.rs

### Implementation Notes
No behavior change intended; focus on maintainability.

### Dependencies
- CLI-001

### Acceptance Criteria
- [ ] main.rs reduced to bootstrap + dispatch wiring.
- [ ] All CLI tests continue passing.
- [ ] Command modules are under 400 lines each where practical.
