## BASE-001 Split base parser into AST and evaluator modules

Status: todo

### Objective
Refactor base parsing into maintainable modules without changing behavior.

### Scope
tao-sdk-bases crate module structure and unit tests.

### Concrete Steps
1. Create modules for lexer, parser, ast, validation, evaluator.
2. Move existing logic out of monolithic lib.rs into those modules.
3. Expose a thin public API facade in lib.rs.
4. Port and extend tests to new module boundaries.

### Required Files and Locations
- crates/tao-sdk-bases/src/lib.rs
- crates/tao-sdk-bases/src/ast.rs
- crates/tao-sdk-bases/src/parser.rs

### Implementation Notes
No semantic changes in this ticket; only structure and tests.

### Dependencies
- none

### Acceptance Criteria
- [ ] tao-sdk-bases compiles with module split and no public API regressions.
- [ ] All existing base tests pass.
- [ ] New module-level tests validate parser/evaluator boundaries.
