# Parity Scope Map (Now / Next / Later)

## Now (Implemented)

- compact CLI IA with one-word groups (`vault/doc/base/graph/meta/task/query`)
- frontmatter + body wikilink indexing parity
- graph diagnostics (`unresolved`, `deadends`, `orphans`, `components`, `walk`)
- metadata aggregations (`properties`, `tags`, `aliases`, `tasks`)
- task extraction and checkbox state transitions with write gate
- deterministic synthetic vault generation and validation

## Next

- full planner-level projection/ranking/explain for all query adapters
- relation-aware base typing and richer schema introspection
- persistent daemon runtime (`taod`) and warm client mode
- incremental reindex and hot-query cache budgets in CI

## Later

- sync and recovery with retention policy/versioning
- app-shell controls parity (tabs/workspace/plugin/theme command surfaces)
- advanced task workflows (priority, recurrence, assignees, rollups)
