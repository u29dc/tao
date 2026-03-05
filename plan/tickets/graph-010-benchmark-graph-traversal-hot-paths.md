## GRAPH-010 Benchmark graph traversal hot paths

Status: done

### Objective
Track graph-read performance across critical commands and prevent regressions.

### Scope
Benchmark harness and budget integration.

### Concrete Steps
1. Add hyperfine cases for graph neighbors, walk, path, unresolved, deadends, and orphans.
2. Export machine-readable summaries under .benchmarks/reports.
3. Define p50 budget targets in plan/perf-budgets.json.
4. Fail budget script when thresholds are exceeded.

### Required Files and Locations
- scripts/bench.sh
- scripts/budgets.sh
- plan/perf-budgets.json

### Implementation Notes
Use generated 10k vault profile as baseline.

### Dependencies
- GRAPH-005
- GRAPH-006
- GRAPH-008

### Acceptance Criteria
- [x] Graph benchmarks run via bun run bench:cli or bench:sdk as applicable.
- [x] Budget report includes graph command p50/p95 metrics.
- [x] Regression causes non-zero exit in budget gate.
