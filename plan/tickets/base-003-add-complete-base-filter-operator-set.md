## BASE-003 Add complete base filter operator set

Status: todo

### Objective
Support rich filtering semantics in base views and query adapter.

### Scope
Operator parser and evaluator.

### Concrete Steps
1. Implement operators: eq, neq, gt, gte, lt, lte, contains, in, starts_with, ends_with.
2. Map operators to typed comparator functions.
3. Add parser validation for unknown operators.
4. Add unit tests for each operator with null and type mismatch behavior.

### Required Files and Locations
- crates/tao-sdk-bases/src/parser.rs
- crates/tao-sdk-bases/src/evaluator.rs
- vault/views/projects.base

### Implementation Notes
Operator semantics must match query adapter behavior.

### Dependencies
- BASE-001

### Acceptance Criteria
- [ ] All supported operators evaluate correctly in unit tests.
- [ ] Unknown operator yields clear parse error.
- [ ] Base view command returns expected filtered rows for fixture cases.
