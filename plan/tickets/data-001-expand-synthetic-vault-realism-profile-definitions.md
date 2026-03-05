## DATA-001 Expand synthetic vault realism profile definitions

Status: done

### Objective
Model realistic graph, metadata, and base usage patterns while keeping fixtures synthetic and private.

### Scope
fixtures generator profile logic and README docs.

### Concrete Steps
1. Define profile schemas for 1k, 2k, 5k, and 10k with deterministic distributions.
2. Include project, meeting, contact, company, daily, and reference note classes.
3. Generate body/frontmatter wikilinks, tags, tasks, aliases, and unresolved ratios.
4. Document profiles and seed behavior in vault/README.md.

### Required Files and Locations
- scripts/fixtures.sh
- vault/README.md
- vault/generated

### Implementation Notes
Do not copy or derive direct content from real personal vault files.

### Dependencies
- none

### Acceptance Criteria
- [x] fixtures.sh can generate 1k, 2k, 5k, and 10k deterministic profiles.
- [x] Generated vaults contain mixed realistic note categories and metadata.
- [x] Profile documentation includes counts, ratios, and seed determinism.
