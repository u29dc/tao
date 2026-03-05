## QUERY-006 Implement streaming query output path

Status: done

### Objective
Reduce latency and memory for large result sets by streaming serialized rows.

### Scope
CLI output writer and service streaming iterator.

### Concrete Steps
1. Add streaming iterator in query execution layer.
2. Add CLI flag for streaming JSON output mode.
3. Preserve existing envelope for non-streaming mode.
4. Benchmark streaming vs standard output and record gains.

### Required Files and Locations
- crates/tao-sdk-search/src/execution.rs
- crates/tao-cli/src/main.rs
- scripts/bench.sh

### Implementation Notes
Streaming mode must remain read-only and deterministic.

### Dependencies
- QUERY-003

### Acceptance Criteria
- [x] Streaming mode handles large result sets without materializing full row arrays.
- [x] Existing non-streaming output remains unchanged.
- [x] Bench report includes streaming comparison metrics.
