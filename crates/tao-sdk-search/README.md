## Crate

`tao-sdk-search`

## Purpose

Provide indexed search/query execution primitives over storage-backed note data.

## Public API

- `SearchQueryService`
- Request/response types for query and projected query paths

## Internal Design

- Query request normalization.
- Storage-backed retrieval with projection and pagination.

## Data Flow

Query request -> storage lookup -> ranking/filtering/projection -> page result.

## Dependencies

- Internal: `tao-sdk-storage`
- External: `rusqlite`, `thiserror`

## Testing

- `cargo test -p tao-sdk-search --release`
- Tests cover query matching, pagination, and projection behavior.

## Limits

- CLI syntax and command routing are handled by `tao-cli`.
