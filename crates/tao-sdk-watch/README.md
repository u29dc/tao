## Crate

`tao-sdk-watch`

## Purpose

Provide watcher/reconcile integration helpers for incremental vault consistency.

## Public API

- Reconcile adapter APIs used by CLI/watch flows
- Typed watch/reconcile error surface

## Internal Design

- Lightweight bridge from filesystem change detection to service reconcile calls.
- Keep logic deterministic and storage-safe.

## Data Flow

Watch event/coalesced path set -> reconcile service invocation -> updated index state.

## Dependencies

- Internal: `tao-sdk-service`, `tao-sdk-vault`
- External: `rusqlite`, `thiserror`

## Testing

- `cargo test -p tao-sdk-watch --release`
- Tests validate reconcile-on-change behavior.

## Limits

- Not a full platform-specific watcher implementation layer.
