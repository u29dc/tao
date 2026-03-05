## QUERY-008 Benchmark unified query path end to end

Status: done

### Objective
Track and budget unified query latency for key read workloads.

### Scope
Benchmark script and budget gate updates.

### Concrete Steps
1. Add hyperfine cases for docs, base, graph, meta, and task query scopes.
2. Capture p50/p95 and startup vs daemon comparisons.
3. Update perf budgets for warm and cold query paths.
4. Fail gate on budget regressions.

### Required Files and Locations
- scripts/bench.sh
- scripts/budgets.sh
- plan/perf-budgets.json

### Implementation Notes
Use generated fixtures only; never external vaults.

### Dependencies
- QUERY-007

### Acceptance Criteria
- [x] bench:cli reports unified query metrics for all scopes.
- [x] budgets.sh validates query thresholds.
- [x] Regression triggers non-zero exit.
