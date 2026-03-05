## QUERY-003 Add projection/select support in unified query

Status: done

### Objective
Allow query callers to request explicit columns/fields only.

### Scope
Planner projection stage and CLI select option handling.

### Concrete Steps
1. Add select list parsing to CLI and query request model.
2. Implement projection stage in physical plan.
3. Validate field names per scope and return structured errors on invalid fields.
4. Add contract tests for projection behavior.

### Required Files and Locations
- crates/tao-cli/src/main.rs
- crates/tao-sdk-search/src/physical_plan.rs
- crates/tao-cli/tests/json_contracts.rs

### Implementation Notes
Projection must preserve deterministic field ordering in output rows.

### Dependencies
- QUERY-001

### Acceptance Criteria
- [x] query --select returns only requested fields.
- [x] Unknown fields fail with stable error code.
- [x] Projection works with limit/offset and sort.
