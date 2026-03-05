## QUERY-002 Implement typed where-expression parser

Status: done

### Objective
Support expressive where clauses consistently across docs, graph, base, and meta scopes.

### Scope
Parser + evaluator integration.

### Concrete Steps
1. Define where-expression grammar with typed literal support.
2. Implement parser with clear error spans/messages.
3. Translate parsed expressions into logical plan filters.
4. Add unit tests for valid and invalid expressions.

### Required Files and Locations
- crates/tao-sdk-search/src/parser.rs
- crates/tao-sdk-search/src/logical_plan.rs
- crates/tao-cli/src/main.rs

### Implementation Notes
Keep initial grammar minimal but extensible.

### Dependencies
- QUERY-001

### Acceptance Criteria
- [ ] query --where expressions execute across at least docs and base scopes.
- [ ] Syntax errors include location and message.
- [ ] Tests cover boolean precedence and type mismatch cases.
