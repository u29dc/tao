## REF-001 Refactor tao-sdk-service lib.rs into modules

Status: done

### Objective
Break monolithic tao-sdk-service lib.rs into coherent modules for maintainability.

### Scope
Service crate internal structure and test updates.

### Concrete Steps
1. Create module boundaries for bootstrap, commands, graph, base, query, meta, and task operations.
2. Move logic from lib.rs into modules while preserving public API exports.
3. Keep lib.rs as module wiring plus public facade types.
4. Run full crate tests and update import paths as needed.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-sdk-service/src/
- crates/tao-sdk-service/tests/

### Implementation Notes
No behavior drift; this is structural refactor with parity tests.

### Dependencies
- none

### Acceptance Criteria
- [ ] lib.rs is reduced to wiring/facade responsibilities.
- [ ] New modules have focused responsibilities and clear names.
- [ ] Service crate tests pass with no regressions.
