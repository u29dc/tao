## BASE-006 Implement relation field resolution

Status: done

### Objective
Resolve relation fields to note references and expose resolved targets.

### Scope
Base row relation parsing and lookup against note index.

### Concrete Steps
1. Define relation type in base schema and value encoding.
2. Resolve relation entries via canonical note path lookup.
3. Store unresolved relation diagnostics separately from graph unresolved.
4. Add tests for single, multi, and broken relation values.

### Required Files and Locations
- crates/tao-sdk-bases/src/types.rs
- crates/tao-sdk-service/src/lib.rs
- crates/tao-sdk-storage/src/bases.rs

### Implementation Notes
Relation resolution must not trigger full-text scans.

### Dependencies
- GRAPH-001
- BASE-002

### Acceptance Criteria
- [ ] Relation fields return resolved note targets where available.
- [ ] Broken relation values are reported with reason codes.
- [ ] Performance remains bounded by indexed lookups.
