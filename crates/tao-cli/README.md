## Crate

`tao-cli`

## Purpose

Provide the Tao command-line interface as a thin JSON-first adapter over SDK services.

## Public API

- Binary: `tao`
- Command groups: `config`, `vault`, `doc`, `base`, `graph`, `meta`, `task`, `search`, `query`
- Public vault-content operations are read-only. Tao writes internal index/cache state for `vault open`, `vault reindex`, daemon/watch/cache maintenance, and health synchronization only.
- `search` is the primary graph-aware exploration entrypoint across indexed docs, files, bases/frontmatter, tasks, and links. Use `rg` for raw grep; use `tao search` when index metadata and relationships matter.
- Canonical graph primitives: `graph links` for one-hop link windows, `graph audit` for unresolved links, structural isolates, components, and scoped inbound-link audits.
- Compatibility graph wrappers such as `graph outgoing`, `graph backlinks`, and `graph unresolved` remain callable by direct name lookup but are hidden from help and omitted from the default `tao tools` catalog.
- JSON envelope output for automation by default

## Internal Design

- `src/main.rs` owns command parsing, dispatch, and envelope serialization.
- Business logic remains in SDK crates; CLI should not reimplement domain rules.

## Data Flow

CLI args -> request mapping -> SDK service call -> envelope serialization -> stdout.

## Runtime Semantics

- Normal vault-facing commands may auto-connect to an existing background daemon and auto-start it when unavailable.
- `vault daemon *` commands are inspection and troubleshooting primitives, not the only way daemon mode is entered.
- `config show` reports effective config values, per-field source labels, source inputs, and precedence without opening or migrating SQLite state.
- `vault reindex` honors a vault-root `.taoignore` for Tao knowledge-indexing exclusions; `.gitignore` is not used because Git tracking and knowledge indexing are separate concerns.
- `health` and `vault preflight` are fresh observational diagnostics; they do not reconcile or cache command results.
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
