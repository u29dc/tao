## GRAPH-007 Add strongly and weakly connected component diagnostics

Status: done

### Objective
Provide topology diagnostics beyond orphan/deadend counts.

### Scope
Component detection APIs and CLI command output.

### Concrete Steps
1. Implement weak component detection for undirected projection.
2. Implement strongly connected components for directed graph.
3. Expose command `graph components` with mode selector.
4. Add tests with crafted component fixture topology.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- vault/generated

### Implementation Notes
Use iterative algorithms to avoid recursion overflow on large vaults.

### Dependencies
- GRAPH-005

### Acceptance Criteria
- [ ] graph components returns component counts and member summaries.
- [ ] Weak and strong modes produce distinct results on directed cycles.
- [ ] Performance remains within existing graph budget constraints.
