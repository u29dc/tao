## BASE-010 Add base parity benchmark suite

Status: done

### Objective
Measure base query latency and keep parity features within performance targets.

### Scope
Benchmark harness + budget gate entries for base operations.

### Concrete Steps
1. Add hyperfine cases for base list, base view, base schema, and query-from-base.
2. Write summary artifacts under .benchmarks/reports with p50/p95.
3. Add budget thresholds to perf config.
4. Fail budget script on threshold regressions.

### Required Files and Locations
- scripts/bench.sh
- scripts/budgets.sh
- plan/perf-budgets.json

### Implementation Notes
Reuse generated 10k fixture and avoid external datasets.

### Dependencies
- BASE-009

### Acceptance Criteria
- [x] Base benchmarks execute from single bench entrypoint.
- [x] Budget script includes base metrics and threshold checks.
- [x] Regression in base p50 causes non-zero exit.
