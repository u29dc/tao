## Crate

`tao-cli`

## Purpose

Provide the Tao command-line interface as a thin JSON-first adapter over SDK services.

## Public API

- Binary: `tao`
- Command groups: `vault`, `doc`, `base`, `graph`, `meta`, `task`, `query`
- Graph audit primitive: `graph inbound-scope` for scoped inbound-link counts (attachment audits).
- JSON envelope output for automation by default

## Internal Design

- `src/main.rs` owns command parsing, dispatch, and envelope serialization.
- Business logic remains in SDK crates; CLI should not reimplement domain rules.

## Data Flow

CLI args -> request mapping -> SDK service call -> envelope serialization -> stdout.

## Runtime Semantics

- Normal vault-facing commands may auto-connect to an existing background daemon and auto-start it when unavailable.
- `vault daemon *` commands are inspection and troubleshooting primitives, not the only way daemon mode is entered.
- `health`, `vault stats`, and `vault preflight` are fresh observational diagnostics; they do not reconcile or cache command results.
- `watcher_status` in CLI health snapshots reflects change-monitor state, not daemon lifecycle state by itself.

## Dependencies

- Internal: `tao-sdk-service`, `tao-sdk-search`, `tao-sdk-bases`, `tao-sdk-watch`, `tao-sdk-vault`, `tao-sdk-storage`, `tao-sdk-bridge`
- External: `clap`, `serde`, `serde_json`, `rusqlite`

## Testing

- `cargo test -p tao-cli --release`
- Contract tests validate stable JSON envelope shape and command IDs.

## Limits

- No UI responsibilities.
- No direct SQLite schema ownership.
