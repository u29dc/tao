## SAFE-003 Enforce safety guard in fixture and benchmark scripts

Status: done

### Objective
Ensure benchmark and fixture automation cannot run against non-repository vault paths.

### Scope
scripts/fixtures.sh, scripts/bench.sh, scripts/budgets.sh only.

### Concrete Steps
1. Source scripts/safety.sh in each automation script.
2. Add path assertions for output roots, fixture roots, resolved vaults, db paths, and daemon socket paths.
3. Fail fast before index/bench execution when any path is unsafe.
4. Keep existing CLI behavior unchanged outside script automation.

### Required Files and Locations
- scripts/fixtures.sh
- scripts/bench.sh
- scripts/budgets.sh

### Implementation Notes
This is script-level safety hardening and should not change benchmark semantics.

### Dependencies
- SAFE-002

### Acceptance Criteria
- [x] Each script rejects Dropbox/personal paths with an explicit safety error.
- [x] Each script still succeeds with vault/generated paths.
- [x] Existing benchmark outputs remain written under .benchmarks/reports.
