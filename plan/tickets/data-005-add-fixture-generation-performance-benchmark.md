## DATA-005 Add fixture generation performance benchmark

Status: done

### Objective
Track generation throughput so large synthetic vault creation remains practical.

### Scope
Fixture script timing and benchmark report output.

### Concrete Steps
1. Measure generation duration for 1k, 5k, and 10k profiles.
2. Export metrics to .benchmarks/reports fixture-generation summary file.
3. Add optional budget thresholds for generation runtime.
4. Document tradeoffs for realism vs generation cost.

### Required Files and Locations
- scripts/fixtures.sh
- scripts/bench.sh
- .benchmarks/reports

### Implementation Notes
Keep generation deterministic; avoid network dependencies.

### Dependencies
- DATA-001
- DATA-002

### Acceptance Criteria
- [ ] Fixture generation benchmark outputs profile-level durations.
- [ ] Report is produced from single bench entrypoint.
- [ ] Optional runtime budgets are enforceable in CI/local flows.
