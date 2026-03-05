## GRAPH-003 Add case-policy aware link canonicalization

Status: done

### Objective
Honor vault case policy when canonicalizing link targets.

### Scope
Canonical key generation and resolver lookup behavior.

### Concrete Steps
1. Thread case policy from config into link canonicalization.
2. Implement deterministic lowercase folding only when case-insensitive mode is active.
3. Add resolver tests for case-sensitive and case-insensitive fixtures.
4. Verify backlinks/outgoing stability across policies.

### Required Files and Locations
- crates/tao-sdk-vault/src/path.rs
- crates/tao-sdk-links/src/lib.rs
- crates/tao-sdk-service/src/config.rs

### Implementation Notes
Do not change path display values in user-facing output.

### Dependencies
- GRAPH-001

### Acceptance Criteria
- [x] Case-insensitive policy resolves mixed-case links to a single canonical note.
- [x] Case-sensitive policy preserves distinct targets when file names differ by case.
- [x] Policy behavior is covered by automated tests.
