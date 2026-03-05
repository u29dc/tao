## Deep Review Checklist

- [ ] Every ticket has objective, scope, explicit steps, file targets, implementation notes, and acceptance criteria.
- [ ] Every acceptance criterion has direct evidence in tests or command output.
- [ ] No placeholder TODOs remain in implementation files changed by tickets.
- [ ] Edge-case tests exist for parsing, path safety, and graph/base/query correctness.
- [ ] Benchmark outputs are deterministic and written under `.benchmarks/reports/`.
- [ ] No references to Dropbox/personal paths outside `plan/archive/`.
- [ ] `bun run util:check` passes.
- [ ] Release build artifacts are produced by `bun run build`.
