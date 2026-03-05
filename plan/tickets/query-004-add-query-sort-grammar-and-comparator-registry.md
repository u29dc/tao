## QUERY-004 Add query sort grammar and comparator registry

Status: done

### Objective
Standardize sorting behavior across all query scopes.

### Scope
Sort parser and comparator mapping by field type.

### Concrete Steps
1. Implement sort argument grammar supporting multi-key sort.
2. Map each key to a typed comparator with null policy.
3. Apply stable tie-breaker by canonical path/id.
4. Add tests for mixed sort keys and null values.

### Required Files and Locations
- crates/tao-sdk-search/src/parser.rs
- crates/tao-sdk-search/src/execution.rs
- crates/tao-sdk-service/tests/conformance_harness.rs

### Implementation Notes
Keep sort behavior consistent with base sort semantics.

### Dependencies
- QUERY-001

### Acceptance Criteria
- [ ] query --sort supports multiple keys.
- [ ] Sorting is deterministic under ties and nulls.
- [ ] Integration tests cover docs and base scopes.
