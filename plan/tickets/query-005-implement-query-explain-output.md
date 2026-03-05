## QUERY-005 Implement query explain output

Status: done

### Objective
Expose planner/executor decisions for debugging and optimization.

### Scope
Explain metadata generation and CLI surface.

### Concrete Steps
1. Add explain flag to query command.
2. Emit logical plan, physical plan, and selected indexes/adapters.
3. Keep explain payload machine-readable JSON.
4. Add tests to ensure explain output is deterministic.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-sdk-search/src/logical_plan.rs
- crates/tao-sdk-search/src/physical_plan.rs

### Implementation Notes
Do not expose internal sensitive paths; only logical identifiers.

### Dependencies
- QUERY-001

### Acceptance Criteria
- [ ] query --explain returns plan metadata without executing full data fetch when not requested.
- [ ] Explain output includes adapter names and filter stages.
- [ ] Snapshot tests validate stable explain payload shape.
