## GRAPH-002 Implement heading and block reference normalization

Status: done

### Objective
Normalize wikilink targets containing headings and block references consistently.

### Scope
Link canonicalization pipeline and tests.

### Concrete Steps
1. Parse `[[note#heading]]` and `[[note#^block]]` forms into structured target components.
2. Apply canonical slug normalization for heading lookups.
3. Retain block identifiers without lossy transformations.
4. Extend outgoing/backlink query outputs to include subtarget metadata.

### Required Files and Locations
- crates/tao-sdk-links/src/lib.rs
- crates/tao-sdk-storage/src/links.rs
- crates/tao-sdk-service/tests/conformance_harness.rs

### Implementation Notes
Preserve raw target for debugging while indexing canonical form for matching.

### Dependencies
- GRAPH-001

### Acceptance Criteria
- [x] Heading links resolve to the same note edge as plain note links with subtarget metadata preserved.
- [x] Block links are indexed and queryable without collisions.
- [x] New tests cover mixed heading and block link variants.
