## DOC-001 Define uniform crate README template

Status: done

### Objective
Create one concise, information-dense README structure used by every crate.

### Scope
Template/spec only.

### Concrete Steps
1. Create README template with fixed H2 sections and no decorative formatting.
2. Include sections: Purpose, Public API, Internal Design, Data Flow, Dependencies, Testing, Limits.
3. Document writing rules (no emojis, concise technical style, no stale ticket references).
4. Publish template under plan/templates and reference in AGENTS.

### Required Files and Locations
- plan/templates/crate-readme-template.md
- AGENTS.md

### Implementation Notes
Template should optimize AI-agent and maintainer comprehension.

### Dependencies
- none

### Acceptance Criteria
- [x] Template file exists with required sections.
- [x] AGENTS references the template for crate documentation consistency.
- [x] Template contains no project-specific stale IDs.
