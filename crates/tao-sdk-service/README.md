## Crate

`tao-sdk-service`

## Purpose

Implement Tao domain services for vault ingest/indexing, graph operations, base execution, metadata/task operations, and reconcile flows.

## Public API

- Indexing/reconcile services
- Note CRUD/property update services
- Graph diagnostics/traversal services
- Base table execution and validation services
- Health snapshot and bootstrap helpers

## Internal Design

- Service-layer orchestration over storage, markdown, links, properties, and vault scanners.
- Parallel scan/parse stages where beneficial, single-writer persistence through storage transactions.
- Stable error enums per service domain.

## Data Flow

Vault scan -> markdown parse/property/link extraction -> canonical resolution -> storage upserts -> query/graph/base read services.

## Dependencies

- Internal: `tao-sdk-bases`, `tao-sdk-config`, `tao-sdk-core`, `tao-sdk-links`, `tao-sdk-markdown`, `tao-sdk-properties`, `tao-sdk-storage`, `tao-sdk-vault`
- External: `rusqlite`, `serde`, `serde_json`, `serde_yaml`, `blake3`, `rayon`, `tracing`, `uuid`, `thiserror`

## Testing

- `cargo test -p tao-sdk-service --release`
- Includes extensive unit tests and `tests/conformance_harness.rs` integration coverage.

## Limits

- Keep transport/UI concerns out of this crate.
- Maintain deterministic outputs for automation and snapshot tests.
