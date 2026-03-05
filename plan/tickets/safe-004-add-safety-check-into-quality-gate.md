## SAFE-004 Add safety check into quality gate

Status: done

### Objective
Make forbidden path scanning mandatory before lint/test/build checks.

### Scope
package.json scripts and related quality gate wiring.

### Concrete Steps
1. Add util:safety script command to package.json.
2. Insert util:safety as the first step in util:check.
3. Verify util:check fails on forbidden literals and passes when clean.
4. Document safety gate in AGENTS command section if needed.

### Required Files and Locations
- package.json
- AGENTS.md

### Implementation Notes
Do not add heavy benchmarks to util:check.

### Dependencies
- SAFE-002

### Acceptance Criteria
- [x] bun run util:safety executes scripts/safety.sh --check-repo.
- [x] bun run util:check runs util:safety first.
- [x] A forbidden literal outside ignored paths causes util:check to fail.
