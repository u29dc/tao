# SQLite Pragma Profile (v1)

## Purpose

Define the default SQLite runtime profile for local vault workloads so every SDK adapter path uses the same durability and latency trade-offs.

## Applied Location

- `tao-sdk-storage::run_migrations` applies this profile during connection startup.
- The profile is applied before migrations and therefore affects bridge, CLI, service, and test entrypoints that call `run_migrations`.

## Selected Profile

| Pragma | Value | Rationale |
| --- | --- | --- |
| `foreign_keys` | `ON` | Enforce referential integrity for repository writes. |
| `journal_mode` | `WAL` | Improve concurrent read/write behavior and reduce writer stalls on local workloads. |
| `synchronous` | `NORMAL` | Reduce fsync overhead while retaining WAL durability semantics appropriate for local cache/index data. |
| `temp_store` | `MEMORY` | Keep transient sort/temp pages in memory for lower query latency. |
| `cache_size` | `-20000` | Reserve approximately 20MB page cache to reduce disk churn on repeated reads. |
| `wal_autocheckpoint` | `1000` | Bound WAL growth with predictable checkpoint cadence. |
| `busy_timeout` | `5000` | Reduce transient lock failures during bursty write and benchmark flows. |

## Validation

- Unit test: `run_migrations_applies_sqlite_pragma_profile_for_file_database` in `crates/tao-sdk-storage/src/lib.rs`.
- Perf alignment: profile selected for `PERF-004` after baseline captures in `PERF-001` to `PERF-003`.
