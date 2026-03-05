## Phase 24 Execution Plan

### Objective
Deliver production-grade Tao core parity for graph exploration, bases/query ergonomics, and maintainability, while preserving current latency budgets and strict repository-local safety.

### Hard Safety Constraint
- Never read from or write to real personal folders.
- Forbidden roots:
  - `/Users/han/Library/CloudStorage/Dropbox/**`
  - `/Users/han/Dropbox/**`
  - any non-repository path for automated tests/benchmarks.
- Allowed roots for automated work:
  - `vault/`
  - `vault/generated/**`
  - temporary directories under repository root only.
- Enforcement:
  - `scripts/safety.sh --check-repo`
  - path assertions in `scripts/fixtures.sh`, `scripts/bench.sh`, and `scripts/budgets.sh`
  - `bun run util:check` runs safety check first.

### Structure
- `plan/tickets/` individual executable ticket specs.
- `plan/tickets.csv` machine-readable index.
- `plan/tickets.json` full structured ticket payload.
- `plan/checklists/execution.md` required execution sequence.
- `plan/checklists/review.md` mandatory deep-review checklist.
- `plan/templates/ticket-template.md` canonical ticket format.
- `plan/run-state.json` execution state.
- `plan/progress.md` evidence log.

### Execution Policy
- Execute tickets in `plan/tickets.csv` order unless a dependency blocks.
- For each ticket:
  - implement complete behavior,
  - add tests for primary path and edge cases,
  - run quality gate subset + affected benchmark/tests,
  - capture evidence in `plan/progress.md`.
- Do not close a ticket if any acceptance criterion is unverified.

### Quality Gates
- Required: `bun run util:check`.
- Required (when ticket affects benchmarks): `bun run bench` or focused benchmark command.
- Required (when ticket affects fixtures): `./scripts/fixtures.sh --profile 10k --seed 42`.

### Archive
Historical plan files are preserved at `plan/archive/phase23-legacy-20260305/`.
