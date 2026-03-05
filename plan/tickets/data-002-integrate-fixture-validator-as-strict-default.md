## DATA-002 Integrate fixture validator as strict default

Status: done

### Objective
Guarantee generated fixtures meet structural and safety invariants before any bench/test run.

### Scope
fixtures validator logic and failure diagnostics.

### Concrete Steps
1. Enforce invariants for links, frontmatter links, unresolved ratio, tasks, tags, and required base files.
2. Fail validation for hub-like files or personal path leakage markers.
3. Keep --skip-validate as explicit opt-out only for debugging.
4. Add validator self-tests with injected failure fixtures.

### Required Files and Locations
- scripts/fixtures.sh
- scripts/tests/fixtures_validator_test.sh

### Implementation Notes
Validation output should include failing invariant and vault path.

### Dependencies
- DATA-001

### Acceptance Criteria
- [x] fixtures.sh fails with clear message on each invariant breach.
- [x] Validator tests include at least three negative cases.
- [x] Default fixture generation path runs validation automatically.
