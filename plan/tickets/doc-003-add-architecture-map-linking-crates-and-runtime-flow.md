## DOC-003 Add architecture map linking crates and runtime flow

Status: done

### Objective
Provide one compact architecture map for agents and maintainers to navigate crate interactions.

### Scope
Top-level AGENTS architecture section + one plan artifact.

### Concrete Steps
1. Create a dependency and data-flow map from CLI to SDK service/storage/bridge crates.
2. Document primary read and write paths with crate boundaries.
3. Include benchmark and fixture flow paths.
4. Link map from AGENTS and crate READMEs where relevant.

### Required Files and Locations
- AGENTS.md
- plan/architecture-map.md
- crates/*/README.md

### Implementation Notes
Use concise lists and minimal diagrams; keep it text-dense.

### Dependencies
- DOC-002

### Acceptance Criteria
- [x] Architecture map file exists and matches current code organization.
- [x] AGENTS links to the map.
- [x] At least core crates reference their place in the runtime flow.
