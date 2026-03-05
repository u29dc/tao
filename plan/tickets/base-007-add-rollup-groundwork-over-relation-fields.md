## BASE-007 Add rollup groundwork over relation fields

Status: todo

### Objective
Enable computed rollup values sourced from related rows.

### Scope
Schema support and evaluator for first rollup operations.

### Concrete Steps
1. Add rollup field definition syntax tied to relation field and target property.
2. Implement rollup functions: count, min, max, sum for numeric fields.
3. Cache intermediate relation traversals within request scope.
4. Add fixture and tests for projects-to-meetings rollups.

### Required Files and Locations
- crates/tao-sdk-bases/src/ast.rs
- crates/tao-sdk-bases/src/evaluator.rs
- vault/views/projects.base

### Implementation Notes
Keep rollup implementation incremental and deterministic.

### Dependencies
- BASE-006

### Acceptance Criteria
- [ ] Rollup fields compute correct values in integration tests.
- [ ] Invalid rollup definitions return clear parse/validation errors.
- [ ] No N+1 relation query pattern in profiler/bench traces.
