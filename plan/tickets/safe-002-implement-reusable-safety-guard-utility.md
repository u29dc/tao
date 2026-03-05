## SAFE-002 Implement reusable safety guard utility

Status: done

### Objective
Provide one script with deterministic path validation and forbidden-literal scanning for all automation flows.

### Scope
New utility script and helper functions only.

### Concrete Steps
1. Create scripts/safety.sh with assert-path and repository scan modes.
2. Implement forbidden-path detection for Dropbox/personal paths.
3. Implement repository-local path assertion against repo root.
4. Add usage help and non-zero exit behavior for all violations.

### Required Files and Locations
- scripts/safety.sh

### Implementation Notes
Design script to be sourceable by other shell scripts and executable standalone.

### Dependencies
- SAFE-001

### Acceptance Criteria
- [x] scripts/safety.sh --assert-path vault/generated exits 0.
- [x] scripts/safety.sh --assert-path /Users/han/Library/CloudStorage/Dropbox/VAULT exits non-zero.
- [x] scripts/safety.sh --check-repo exits non-zero when a forbidden literal is injected into a temp file outside ignore globs.
