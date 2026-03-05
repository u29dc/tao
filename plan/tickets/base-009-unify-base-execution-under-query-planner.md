## BASE-009 Unify base execution under query planner

Status: done

### Objective
Route base command execution through shared query pipeline to avoid duplicated logic.

### Scope
Service-layer query adapter integration.

### Concrete Steps
1. Define base adapter for shared planner scan/filter/sort/project stages.
2. Replace direct base evaluator command path with planner invocation.
3. Add explain metadata to confirm adapter usage.
4. Update tests to assert planner-backed execution equivalence.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-sdk-bases/src/lib.rs
- crates/tao-sdk-search/src/lib.rs

### Implementation Notes
Keep output schema stable while switching execution engine.

### Dependencies
- BASE-003
- BASE-004
- QUERY-001

### Acceptance Criteria
- [ ] base view results match pre-migration outputs for fixture set.
- [ ] Execution metadata indicates planner path.
- [ ] No duplicate evaluator path remains in service code.
