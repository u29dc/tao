## SAFE-005 Add automated safety regression tests

Status: done

### Objective
Protect safety behavior from regressions with deterministic automated tests.

### Scope
Shell-level integration tests for safety script and guarded script entrypoints.

### Concrete Steps
1. Add a shell test harness under scripts/tests for safety assertions.
2. Test allowed repository-local path assertions.
3. Test forbidden Dropbox path rejection.
4. Test repository scan mode against controlled temp fixtures.

### Required Files and Locations
- scripts/tests/safety_test.sh
- scripts/safety.sh

### Implementation Notes
Use mktemp under repository root; clean up after test run.

### Dependencies
- SAFE-003
- SAFE-004

### Acceptance Criteria
- [x] scripts/tests/safety_test.sh exits 0 when safety behavior is correct.
- [x] At least one negative test proves forbidden path rejection.
- [x] Test script is runnable from repository root in CI-compatible shell.
