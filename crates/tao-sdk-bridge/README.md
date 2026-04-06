## Crate

`tao-sdk-bridge`

## Purpose

Expose Tao SDK capabilities through a stable internal bridge surface for CLI runtime and benchmark consumers.

## Public API

- `BridgeKernel` read/write methods (`vault_stats`, `note_get`, `notes_list`, `note_links`, `note_context`, `bases_*`, `note_put`, `events_poll`)
- Envelope types: `BridgeEnvelope`, `BridgeError`

## Internal Design

- Central kernel wraps service/storage dependencies.
- Structured error mapping with stable codes.
- Rust-first DTO layer for internal transport boundaries.

## Data Flow

Bridge request -> kernel call -> SDK services -> typed envelope serialization.

## Dependencies

- Internal: `tao-sdk-service`, `tao-sdk-storage`, `tao-sdk-bases`, `tao-sdk-vault`, `tao-sdk-markdown`
- External: `serde`, `serde_json`, `rusqlite`, `blake3`, `thiserror`

## Testing

- `cargo test -p tao-sdk-bridge --release`
- Tests cover schema compatibility, note read/write, links, bases, and event polling.

## Limits

- Bridge should remain transport-focused and avoid duplicating service-layer policy logic.
