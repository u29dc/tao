## Crate

`tao-sdk-storage`

## Purpose

Own SQLite schema, migrations, and repository-level persistence primitives for Tao.

## Public API

- Migration APIs (`apply_initial_schema`, `preflight_migrations`, `run_migrations`)
- Repository modules: files, links, properties, bases, tasks, search index, render cache, index state
- Transaction helper utilities

## Internal Design

- SQL-focused repository modules with typed row mappings.
- Deterministic migration management with checksum verification.

## Data Flow

Service request -> repository call -> SQL transaction -> typed result.

## Dependencies

- External: `rusqlite`, `blake3`, `thiserror`

## Testing

- `cargo test -p tao-sdk-storage --release`
- Tests cover migrations, repositories, cascades, and transaction semantics.

## Limits

- Business rules belong in service layer, not storage repositories.
