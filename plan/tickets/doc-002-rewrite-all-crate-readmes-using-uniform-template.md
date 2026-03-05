## DOC-002 Rewrite all crate READMEs using uniform template

Status: done

### Objective
Replace placeholder or stale README content with consistent, accurate crate architecture docs.

### Scope
All crate README.md files under crates/.

### Concrete Steps
1. Inventory all crate README files and map each to current crate behavior.
2. Rewrite each README with the standard section layout.
3. Remove outdated ticket IDs and obsolete claims.
4. Cross-check README API references against actual code exports.

### Required Files and Locations
- crates/*/README.md
- plan/templates/crate-readme-template.md

### Implementation Notes
Keep documentation concise and avoid implementation fiction.

### Dependencies
- DOC-001

### Acceptance Criteria
- [x] Every crate has a non-placeholder README with the same section structure.
- [x] No stale ticket IDs remain in crate READMEs.
- [x] README API names match actual crate exports and command surfaces.
