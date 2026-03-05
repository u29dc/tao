## REF-002 Refactor tao-sdk-service indexing.rs into submodules

Status: done

### Objective
Split indexing pipeline into explicit stages for scan, parse, resolve, and persist.

### Scope
Indexing implementation organization and internal APIs.

### Concrete Steps
1. Create indexing modules for file_scan, parse_extract, link_resolve, write_batch, reconcile.
2. Extract shared structs for stage inputs/outputs.
3. Preserve parallelism and batching behavior while improving readability.
4. Add stage-level unit tests where absent.

### Required Files and Locations
- crates/tao-sdk-service/src/indexing.rs
- crates/tao-sdk-service/src/indexing/
- crates/tao-sdk-service/tests/

### Implementation Notes
Keep existing performance optimizations intact.

### Dependencies
- REF-001

### Acceptance Criteria
- [ ] indexing.rs no longer contains monolithic stage logic.
- [ ] Each indexing stage has dedicated module and tests.
- [ ] Indexing integration tests still pass and performance does not regress materially.
