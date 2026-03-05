## REF-004 Extract shared parsing and normalization utilities

Status: done

### Objective
Reduce duplicated logic across crates by introducing shared utility modules/crate.

### Scope
Cross-crate utility extraction for tokenization, normalization, and deterministic sorting.

### Concrete Steps
1. Identify duplicated helpers in links, query, base, and service crates.
2. Extract shared helpers into tao-sdk-core or dedicated utility module.
3. Replace duplicate call sites with shared utility usage.
4. Add unit tests for utility behavior and migration parity checks.

### Required Files and Locations
- crates/tao-sdk-core/src/
- crates/tao-sdk-links/src/lib.rs
- crates/tao-sdk-search/src/lib.rs

### Implementation Notes
Do not over-abstract; extract only stable, repeated primitives.

### Dependencies
- GRAPH-001
- QUERY-001
- BASE-001

### Acceptance Criteria
- [ ] Duplicate utility code is reduced across targeted crates.
- [ ] Shared utility tests cover canonicalization and deterministic ordering.
- [ ] All dependent crates compile and pass tests after extraction.
