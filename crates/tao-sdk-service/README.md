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
- Includes extensive unit tests and `tests/conformance.rs` integration coverage.

## Limits

- Keep transport/UI concerns out of this crate.
- Maintain deterministic outputs for automation and snapshot tests.

## Publication and content

Indexing captures one stable source revision for Markdown projections and TXT text. A final inventory is prepared before graph resolution; logical batch boundaries do not expose partial graph/search state. Canonical and derived generation counters fence cache reuse, while watcher/fallback reconciliation detects external filesystem changes separately.

`ContentIndexService` prepares supported content and persists revision-keyed jobs. Local PDF workers claim leased jobs, extract outside the publication transaction, and publish only if the source and desired extraction identity still match. Last-good content remains explicitly stale when replacement extraction fails. File format/group metadata exists independently of extraction support.

File-backed extraction uses separate SQLite connections in a CPU-sized pool, capped at eight leased jobs across all processes sharing the database. Each subprocess retains its own memory/scratch limits; those limits are per worker. A reentrant, cross-process publication guard prevents background content changes from invalidating an in-flight source preparation. The guard never spans OCR and is not required for readers. Deferred PDF captures stay in their durable queue until capacity is available; unchanged deferred sources do not trigger a fresh reconciliation. Native text is extracted in batches of up to 32 physical pages, with an exact-page fallback for unverified boundaries or oversized batch output.

`read_content` returns bounded source-addressed segments and revision-bound continuation. `content_spool_root` derives isolated internal storage from the actual database identity. Source originals are never modified. The content tests exercise native text, OCR, physical-page identity, cancellation and coverage reporting.

Atomic preparation admits at most 256 MiB of charged retained capture/projection capacities; reconciliation keeps at most 64 MiB of source snapshots. The charge includes Markdown raw/body/structure, properties/tasks/links, Base source JSON and TXT/PDF prepared publications. Exceeding it returns `PreparationBudgetExceeded` before the writer transaction and preserves the previous index. Narrow the included content or submit smaller changed-path sets before retrying. A fresh rebuild must fit this supported work set; disk staging for larger atomic rebuilds is not implemented.

This admission limit is not a process RSS ceiling. One current source is independently limited to 32 MiB for Markdown/Base/TXT and 64 MiB for PDF; parser/decoder scratch, SQLite, allocator overhead and inventory/graph metadata have separate lifetimes. `last_incremental_index_summary.work` records successful publication counters: consumed source captures/bytes, Markdown parses, canonical structures loaded/reparsed, resolved graph sources, transactions and peak preparation/capture charges. Capture counters cover the apply stage and exclude unchanged verification reads performed while detecting drift.
