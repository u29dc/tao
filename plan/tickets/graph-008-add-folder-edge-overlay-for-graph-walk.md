## GRAPH-008 Add folder-edge overlay for graph walk

Status: done

### Objective
Support combined semantic traversal over links and folder hierarchy without synthetic hub notes.

### Scope
Graph walk edge-expansion behavior and CLI options.

### Concrete Steps
1. Add optional folder-parent and sibling edge generation to traversal.
2. Expose flag `--include-folders` for graph walk command.
3. Ensure traversal output labels edge type (wikilink, folder-parent, folder-sibling).
4. Add deterministic test fixtures for mixed edge traversal.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- crates/tao-bench/src/main.rs

### Implementation Notes
Default behavior remains wikilink-only to preserve existing contract.

### Dependencies
- GRAPH-005

### Acceptance Criteria
- [ ] graph walk with --include-folders traverses folder relationships without hub notes.
- [ ] Edge type labels are present in output.
- [ ] Without the flag, traversal matches current behavior.
