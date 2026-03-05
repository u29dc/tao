## SAFE-001 Codify repository-local vault safety policy

Status: done

### Objective
Make the non-negotiable local-only vault safety policy explicit in repository operating instructions and phase plan artifacts.

### Scope
Policy text only; no runtime behavior changes in this ticket.

### Concrete Steps
1. Add a dedicated safety section in AGENTS.md that forbids Dropbox and personal-folder access for automated work.
2. Add matching safety constraints in plan/README.md and execution checklist artifacts.
3. Define allowed paths and forbidden paths explicitly with absolute examples.
4. Document violation behavior as hard-fail and stop execution.

### Required Files and Locations
- AGENTS.md
- plan/README.md
- plan/checklists/execution.md

### Implementation Notes
This ticket establishes policy only; enforcement is implemented in SAFE-002..SAFE-004.

### Dependencies
- none

### Acceptance Criteria
- [x] AGENTS.md contains a dedicated hard safety rule section with forbidden and allowed path patterns.
- [x] plan/README.md repeats the same constraints without contradiction.
- [x] Execution checklist contains an explicit safety preflight item.
