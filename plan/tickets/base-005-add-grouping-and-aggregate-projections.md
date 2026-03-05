## BASE-005 Add grouping and aggregate projections

Status: done

### Objective
Enable grouped base views with aggregate columns for count/sum/min/max.

### Scope
Evaluator aggregation pipeline and CLI output shape.

### Concrete Steps
1. Define group-by syntax and aggregate expression schema.
2. Implement grouped row materialization with deterministic key ordering.
3. Expose grouped output metadata and aggregate columns in base/query commands.
4. Add integration tests with projects and meetings fixtures.

### Required Files and Locations
- crates/tao-sdk-bases/src/ast.rs
- crates/tao-sdk-bases/src/evaluator.rs
- crates/tao-cli/src/main.rs

### Implementation Notes
Keep grouped and flat output modes clearly separated.

### Dependencies
- BASE-003
- BASE-004

### Acceptance Criteria
- [ ] Base views can return grouped aggregates with stable schema.
- [ ] Aggregate functions compute correct values in tests.
- [ ] JSON output includes grouping metadata.
