# Tao Performance and Correctness Completion Plan

This file records the six review findings, the implemented fixes, and the success criteria used to validate them. Keep benchmark reports under `.benchmarks/reports/`; do not commit live-vault paths, private query strings, or machine-local runtime data.

## Completion Bar

- Public CLI JSON contracts remain backward-compatible unless tests explicitly cover a new field.
- Public vault-content operations remain read-only.
- Each implemented finding has focused regression coverage.
- Performance claims cite repo-local fixture reports.
- Final validation includes focused crate tests, `cargo test --workspace --release`, and `bun run util:check`.

## Validation Summary

Passed:

- `cargo test -p tao-sdk-properties --release`
- `cargo test -p tao-sdk-storage --release`
- `cargo test -p tao-sdk-service --release`
- `cargo test -p tao-cli --release`
- `bun run util:check`

Benchmark evidence:

- CLI smoke matrix, 1k fixture: `.benchmarks/reports/20260610T185203Z`
- Single-file drift reindex, 5k fixture: `.benchmarks/reports/20260610T185245Z`
- Service search, 5k fixture: `.benchmarks/reports/20260610T185340Z`
- Service search, 1k fixture: `.benchmarks/reports/20260610T185707Z`
- Service search limits, 5k fixture: `.benchmarks/reports/20260610T185740Z-search-service-limits`
- Graph walk, 5k fixture: `.benchmarks/reports/20260610T185438Z`
- Health vs deep health, 5k fixture: `.benchmarks/reports/20260610T185500Z-health`
- Graph path/walk CLI, 5k fixture: `.benchmarks/reports/20260610T185520Z-graph-cli`
- Search CLI limits, 5k fixture: `.benchmarks/reports/20260610T185540Z-search-cli`
- Search storage size comparison, 5k fixture: `.benchmarks/reports/20260610T185820Z-size-compare`

## 1. Partial Search-Corpus Refresh for Single-File Reindex

Status: implemented and validated.

Changed files:

- `crates/tao-sdk-service/src/indexing/pipeline/incremental.rs`
- `crates/tao-sdk-service/src/indexing/pipeline/reconcile_scan.rs`
- `crates/tao-sdk-service/src/search_corpus.rs`
- `crates/tao-sdk-storage/src/files.rs`
- `crates/tao-sdk-storage/src/tasks.rs`
- `crates/tao-sdk-storage/src/search_segments.rs`
- `crates/tao-cli/src/cli_impl/commands/vault.rs`
- `crates/tao-cli/src/cli_impl/tests.rs`

Implementation:

- Added `SearchCorpusRefreshMode` with `none`, `partial`, and `full` result labels.
- Incremental and coalesced reindex now track impacted file IDs and only rebuild selected corpus rows when safe.
- `.base` changes and broad link-resolution invalidation still fall back to full corpus refresh.
- `SearchCorpusService::refresh_files_in_transaction` deletes and rebuilds per-file segments and aliases for impacted files.
- Targeted storage reads load only selected files, tasks, graph rows, and related corpus inputs.
- `vault reindex` output now reports `search_corpus_refresh` while preserving existing compatibility fields.

Success criteria:

- Met: one-file markdown edits report `search_corpus_refresh: "partial"`.
- Met: search sees changed content immediately after partial refresh.
- Met: fallback paths still produce full refresh when global derived rows may be affected.
- Measured: 5k single-file drift reindex mean was `851.982 ms` over 3 runs.
- Caveat: no same-machine pre-change 5k baseline was preserved for a direct speedup ratio.

## 2. Reduce Broad Search Hydration Cost

Status: implemented and validated, with a fixed-cost caveat.

Changed files:

- `crates/tao-sdk-storage/src/search_segments.rs`
- `crates/tao-sdk-service/src/search.rs`
- `crates/tao-cli/src/cli_impl/tests.rs`

Implementation:

- Added lightweight `SearchSegmentCandidate` rows.
- Added batch hydration by `segment_id`.
- Search now ranks candidates before parsing `payload_json`.
- Payload JSON is parsed only for the final selected segment rows.
- Exact totals remain enabled for compatibility.

Success criteria:

- Met: existing search contract tests pass.
- Met: representative alias, context, scope, path, limit, and empty-result tests pass.
- Met: exact totals remain correct.
- Met: full payload parsing is deferred until final hydration.
- Measured: 5k direct service search p50 was `82.728 ms` at limit 10, `94.392 ms` at limit 50, and `102.978 ms` at limit 100.
- Measured: 5k daemon-forwarded CLI search limits were about `5.8-6.0 ms`.
- Caveat: direct service search still has a fixed cost from freshness/status, exact total, alias, and FTS queries. The candidate hydration change improves the variable hydration cost but does not remove that fixed cost.

## 3. Split Fast Health from Deep Scan Health

Status: implemented and validated.

Changed files:

- `crates/tao-cli/src/cli_impl/args.rs`
- `crates/tao-cli/src/cli_impl/commands/health.rs`
- `crates/tao-cli/src/cli_impl/commands/vault.rs`
- `crates/tao-cli/src/cli_impl/registry.rs`
- `crates/tao-sdk-service/src/index_refresh.rs`
- `crates/tao-cli/src/cli_impl/tests.rs`

Implementation:

- Added `health --deep`.
- Default `tao health` uses cached DB/index state and avoids vault content hashing.
- Deep health preserves the existing reconciliation scan behavior.
- `vault stats` uses cached inspection.
- Reindex dry-run and reindex output now report scan mode explicitly.

Success criteria:

- Met: default health avoids full vault scan/hash.
- Met: `health --deep` preserves scan-based diagnostics.
- Met: dry-run/reindex output includes scan mode.
- Measured: 5k default health mean was `8.594 ms`; `health --deep` mean was `64.407 ms`; default health was about `7.49x` faster.

## 4. Consolidate Duplicated Search Storage

Status: implemented and validated.

Changed files:

- `crates/tao-sdk-search/src/execution.rs`
- `crates/tao-cli/src/cli_impl/commands/query.rs`
- `crates/tao-cli/src/cli_impl/query_docs.rs`
- `crates/tao-sdk-service/src/search_corpus.rs`
- `crates/tao-sdk-service/src/indexing/pipeline.rs`
- `crates/tao-sdk-service/src/indexing/pipeline/full.rs`
- `crates/tao-sdk-service/src/indexing/pipeline/incremental.rs`
- `crates/tao-sdk-service/src/indexing/pipeline/errors.rs`
- `crates/tao-sdk-storage/src/lib.rs`
- `crates/tao-sdk-storage/migrations/0009_drop_search_index.sql`
- Deleted `crates/tao-sdk-storage/src/search_index.rs`

Implementation:

- `query --from docs` now reads the `docs` surface from `search_segments`.
- Full and incremental indexing no longer write the legacy `search_index` table.
- Search corpus doc segments are built directly from indexed file records and markdown file content.
- Added migration `0009_drop_search_index` to retire legacy `search_index` and `search_index_fts` after old migrations run.
- Removed the storage repository API for `search_index`.
- Kept public compatibility field names such as `search_index_stale` where existing CLI JSON contracts rely on them.

Success criteria:

- Met: docs query contract tests pass for filters, sorts, selected columns, offsets, and empty/no-query variants.
- Met: full and incremental indexing no longer write `search_index`.
- Met: new databases finish without the legacy table in the final schema.
- Met: existing databases can apply forward migrations and drop the legacy table.
- Measured: search/query benchmarks are saved under `.benchmarks/reports/`.
- Measured: settled 5k DB size dropped from `143,048,704` bytes to `135,995,392` bytes, a `4.94%` total reduction.
- Measured: `dbstat` search-table footprint dropped by `6,430,720` bytes, a `6.90%` search-table reduction.
- Caveat: the original `25%` storage-reduction target was not met because `search_segments`, `search_segments_fts`, and `search_aliases` now dominate search storage; the removed legacy `search_index*` pages were only about `6.5 MB` on the 5k fixture.

## 5. Bound Graph Path and Walk Work

Status: implemented and validated.

Changed files:

- `crates/tao-sdk-service/src/graph.rs`
- `crates/tao-cli/src/cli_impl/commands/graph.rs`
- `crates/tao-cli/src/cli_impl/tests.rs`

Implementation:

- Moved shortest-path traversal into `BacklinkGraphService`.
- `graph path` now uses bounded BFS over frontier queries instead of CLI-owned full-vault adjacency.
- `graph walk` avoids loading all markdown files unless folder overlay output is requested.
- Added traversal guardrails through max depth and max nodes.

Success criteria:

- Met: existing graph parity and contract tests pass.
- Met: path, no-path, guardrail, and folder-walk cases are covered.
- Met: bounded path/walk no longer require full-vault adjacency for ordinary queries.
- Measured: 5k service graph walk warm p50 was `5.868 ms`, cold p50 was `6.499 ms`.
- Measured: 5k CLI graph path mean was `4.8 ms`; graph walk mean was `4.5 ms`, near shell timing limits.

## 6. Add Frontmatter YAML Resource Guards

Status: implemented and validated.

Changed files:

- `crates/tao-sdk-properties/src/lib.rs`
- `crates/tao-sdk-service/src/indexing/pipeline.rs`
- `crates/tao-cli/src/cli_impl/commands/validate.rs`
- `crates/tao-cli/src/cli_impl/tests.rs`

Implementation:

- Added `MAX_FRONT_MATTER_BYTES` and `MAX_FRONT_MATTER_DEPTH`.
- Oversized frontmatter is rejected before full raw YAML allocation/parsing.
- Deep frontmatter traversal is bounded.
- Validation maps oversized frontmatter to stable code `frontmatter.too_large`.
- Existing syntax errors keep backward-compatible malformed-YAML behavior.

Success criteria:

- Met: oversized frontmatter is rejected before YAML parse.
- Met: valid frontmatter at the configured limit remains accepted.
- Met: malformed YAML diagnostics remain stable.
- Met: deeply nested YAML cannot trigger unbounded recursive traversal.

## Residual Follow-Up

- Direct service search still spends most time outside payload hydration. Next high-leverage work should profile freshness/status checks, exact total counting, alias overlap counting, and FTS query planning before adding more code.
- Further DB-size reduction would require changing `search_segments_fts` storage mode or reducing duplicated text/payload columns; dropping `search_index` alone is now complete.
- Public JSON fields named `search_index_stale` remain for compatibility even though the underlying storage is now `search_segments`.
