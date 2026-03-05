## BASE-002 Implement strict typed coercion for base fields

Status: done

### Objective
Guarantee deterministic coercion behavior for string, number, bool, and date-like fields.

### Scope
Base evaluator typing and schema enforcement.

### Concrete Steps
1. Define coercion rules per field type with explicit failure paths.
2. Apply coercion at row materialization before filters/sorts.
3. Return structured errors for invalid coercion when strict mode is enabled.
4. Add fixtures and tests for mixed-type datasets.

### Required Files and Locations
- crates/tao-sdk-bases/src/evaluator.rs
- crates/tao-sdk-bases/src/types.rs
- crates/tao-sdk-service/tests/conformance_harness.rs

### Implementation Notes
Preserve permissive fallback mode where currently expected by existing commands.

### Dependencies
- BASE-001

### Acceptance Criteria
- [ ] Typed coercion is deterministic across runs.
- [ ] Invalid values produce stable error payloads in strict mode.
- [ ] Tests cover number, bool, date, and null edge cases.
