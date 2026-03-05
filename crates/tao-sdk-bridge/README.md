## Crate

`tao-sdk-bridge`

## Purpose

Expose Tao SDK capabilities through a stable bridge surface for CLI daemon mode and UniFFI/Swift consumers.

## Public API

- `BridgeKernel` read/write methods (`vault_stats`, `note_get`, `notes_list`, `note_links`, `note_context`, `bases_*`, `note_put`, `events_poll`)
- Envelope types: `BridgeEnvelope`, `BridgeError`
- Runtime APIs: `TaoBridgeRuntime` and JSON helper functions

## Internal Design

- Central kernel wraps service/storage dependencies.
- Structured error mapping with stable codes.
- UniFFI-safe DTO layer for cross-language boundaries.

## Data Flow

Bridge request -> kernel call -> SDK services -> typed envelope -> JSON/UniFFI serialization.

## Dependencies

- Internal: `tao-sdk-service`, `tao-sdk-storage`, `tao-sdk-bases`, `tao-sdk-vault`, `tao-sdk-markdown`
- External: `serde`, `serde_json`, `rusqlite`, `uniffi`, `thiserror`

## Testing

- `cargo test -p tao-sdk-bridge --release`
- Tests cover schema compatibility, runtime init, note read/write, links, bases, and event polling.

## Limits

- Bridge should remain transport-focused and avoid duplicating service-layer policy logic.
