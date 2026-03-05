## QUERY-001 Split query engine into logical and physical planner modules

Status: todo

### Objective
Refactor query engine architecture for maintainability and future optimizer work.

### Scope
tao-sdk-search module decomposition and interface cleanup.

### Concrete Steps
1. Create modules: logical_plan, physical_plan, optimizer, adapters, execution.
2. Move existing query pipeline logic out of monolithic file.
3. Keep public API backward compatible for current command handlers.
4. Add planner unit tests for plan construction invariants.

### Required Files and Locations
- crates/tao-sdk-search/src/lib.rs
- crates/tao-sdk-search/src/logical_plan.rs
- crates/tao-sdk-search/src/physical_plan.rs

### Implementation Notes
This ticket is structural and should not alter query semantics.

### Dependencies
- none

### Acceptance Criteria
- [ ] Query engine compiles with new modules and existing command tests pass.
- [ ] Planner unit tests validate deterministic plan generation.
- [ ] Public query API remains stable.
