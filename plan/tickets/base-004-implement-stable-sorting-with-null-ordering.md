## BASE-004 Implement stable sorting with null ordering

Status: done

### Objective
Provide deterministic multi-column sorting with explicit null placement.

### Scope
Sort evaluator and contract tests.

### Concrete Steps
1. Add multi-key comparator with stable tie-breaking by canonical path.
2. Support per-column nulls-first/nulls-last behavior with default policy.
3. Expose null ordering behavior in base/query output metadata.
4. Add regression tests for mixed null and numeric/string datasets.

### Required Files and Locations
- crates/tao-sdk-bases/src/evaluator.rs
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/tests/json_contracts.rs

### Implementation Notes
Deterministic output is mandatory for snapshot tests and agent consumption.

### Dependencies
- BASE-002

### Acceptance Criteria
- [ ] Sorting output is stable across repeated runs.
- [ ] Null placement follows configured/default policy.
- [ ] Contract tests verify sort metadata and row order.
