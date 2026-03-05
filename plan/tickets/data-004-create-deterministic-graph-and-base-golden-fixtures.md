## DATA-004 Create deterministic graph and base golden fixtures

Status: done

### Objective
Provide stable fixture subsets dedicated to parity and snapshot tests.

### Scope
New fixture directories and generation hooks.

### Concrete Steps
1. Create dedicated deterministic fixture profiles for graph and base parity tests.
2. Commit compact fixture subsets with known topology and expected outputs.
3. Link fixture generation script to refresh goldens with fixed seed.
4. Document fixture refresh workflow for contributors.

### Required Files and Locations
- vault/fixtures/graph-parity
- vault/fixtures/base-parity
- scripts/fixtures.sh

### Implementation Notes
Keep committed fixture size small while preserving edge-case coverage.

### Dependencies
- DATA-001

### Acceptance Criteria
- [ ] Parity fixture directories are reproducible from fixed seeds.
- [ ] Snapshot tests use parity fixtures instead of ad-hoc notes.
- [ ] Fixture refresh process is documented and deterministic.
