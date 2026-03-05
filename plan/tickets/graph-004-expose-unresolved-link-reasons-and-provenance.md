## GRAPH-004 Expose unresolved-link reasons and provenance

Status: done

### Objective
Improve unresolved diagnostics to explain why a link did not resolve.

### Scope
Graph unresolved query output and indexing metadata.

### Concrete Steps
1. Add reason codes for unresolved links (missing-note, bad-anchor, bad-block, malformed-target).
2. Store origin metadata for unresolved entries (source path and source field).
3. Return reason and provenance in graph unresolved command output.
4. Add tests for each reason code using fixture notes.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-sdk-storage/src/links.rs
- crates/tao-cli/src/main.rs

### Implementation Notes
Use stable reason identifiers suitable for automation.

### Dependencies
- GRAPH-001

### Acceptance Criteria
- [ ] graph unresolved includes reason and source fields for every entry.
- [ ] At least four reason categories are covered by tests.
- [ ] Output remains backward compatible for existing fields.
