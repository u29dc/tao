## GRAPH-006 Add graph shortest-path traversal

Status: done

### Objective
Compute shortest note-to-note paths for semantic graph exploration.

### Scope
Read-only BFS traversal over indexed adjacency.

### Concrete Steps
1. Implement BFS-based shortest path service API with max-depth and max-nodes guardrails.
2. Expose command `graph path --from <note> --to <note>`.
3. Return path node list and edge count in JSON envelope.
4. Add tests for found path, no path, and guardrail limits.

### Required Files and Locations
- crates/tao-sdk-service/src/lib.rs
- crates/tao-cli/src/main.rs
- crates/tao-sdk-service/tests/conformance_harness.rs

### Implementation Notes
Use adjacency from index; avoid full table scans per step.

### Dependencies
- GRAPH-005

### Acceptance Criteria
- [x] graph path returns deterministic shortest routes on fixture graph.
- [x] No-path cases return empty path with explicit status.
- [x] Guardrails prevent runaway traversal on dense graphs.
