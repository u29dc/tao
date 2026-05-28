## Crate

`tao-sdk-service`

## Purpose

Implement Tao domain services for vault ingest/indexing, graph-aware search, base execution, metadata/task reads, and reconcile flows.

## Public API

- Indexing/reconcile services
- Graph-aware vault search orchestration
- Graph diagnostics/traversal services
- Base table execution and validation services
- Health snapshot and bootstrap helpers

## Internal Design

- Service-layer orchestration over storage, markdown, links, properties, and vault scanners.
- Parallel scan/parse stages where beneficial, single-writer persistence through storage transactions.
- `SearchCorpusService` derives the unified `tao search` corpus from canonical index tables after full and incremental indexing. It materializes weighted search segments and exact/normalized aliases so query-time search stays on FTS/indexed lookups instead of per-surface table scans.
- `VaultSearchService` ranks candidates from the unified corpus, hydrates only bounded result sections, and expands context after root selection.
- Stable error enums per service domain.

## Data Flow

Vault scan -> markdown parse/property/link extraction -> canonical resolution -> storage upserts -> search corpus rebuild -> query/graph/base/search read services.

## Dependencies

- Internal: `tao-sdk-bases`, `tao-sdk-config`, `tao-sdk-core`, `tao-sdk-links`, `tao-sdk-markdown`, `tao-sdk-properties`, `tao-sdk-storage`, `tao-sdk-vault`
- External: `rusqlite`, `serde`, `serde_json`, `serde_yaml`, `blake3`, `rayon`, `tracing`, `uuid`, `thiserror`

## Testing

- `cargo test -p tao-sdk-service --release`
- Includes extensive unit tests and `tests/conformance_harness.rs` integration coverage.

## Limits

- Keep transport/UI concerns out of this crate.
- Maintain deterministic outputs for automation and snapshot tests.
