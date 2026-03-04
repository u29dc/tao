## 1. Documentation
- Primary policy: this file is the canonical operational reference for repository architecture, commands, quality gates, and roadmap state.
- External references for tool behavior and standards:
  - `https://bun.sh/docs/llms.txt`
  - `https://swift.org/documentation/`
  - `https://www.rust-lang.org/learn`
  - `https://mozilla.github.io/uniffi-rs/latest/`
- Internal source references:
  - Architecture and roadmap: `plan/PLAN.md`
  - Ticket ledger: `plan/tickets.csv`
  - Run state: `plan/run-state.json`
  - Progress log: `plan/progress.md`
  - Blockers: `plan/blockers.md`

## 2. Repository Structure
```text
apps/
  tao-macos/                    Swift macOS app + generated UniFFI bindings
crates/
  tao-sdk-*/                    Rust SDK domain crates (core, storage, service, bridge, etc.)
  tao-cli/                      Minimal JSON/terminal wrapper over SDK services
  tao-bench/                    Release benchmark harness + budget reports
  tao-tui/                      Placeholder shell (intentionally minimal)
vault/                          Root shipped fixture vault for QA and manual smoke
plan/                           Execution control plane (plan, tickets, run-state, progress)
scripts/                        One-word operational scripts (clean, ffi, budgets, fixtures, package, release)
```

## 3. Stack
| Layer | Choice | Notes |
| --- | --- | --- |
| Core engine | Rust workspace | `unsafe_code = "forbid"` at workspace level |
| Storage | SQLite via `rusqlite` | migrations and bootstrap driven by SDK service/config |
| Native bridge | UniFFI surface in `tao-sdk-bridge` | generated Swift bindings consumed by macOS app |
| CLI | `tao` binary (`clap`) | thin wrapper over SDK APIs with one-envelope JSON |
| macOS UI | SwiftUI | file tree sidebar + note reader + settings window |
| Bench harness | `tao-bench` | bridge/ffi/startup metrics with budget gate script |

## 4. Commands
- `bun run util:clean` -> remove build/runtime artifacts and local install outputs.
- `bun run util:ffi` -> build bridge and regenerate Swift UniFFI bindings.
- `bun run util:build` -> release Rust build + UniFFI generation + release Swift build.
- `bun run util:check` -> format, lint, release check/test, audit, release build.
- `./scripts/budgets.sh` -> bridge/ffi/startup performance budget gate.
- `./scripts/fixtures.sh [output-root]` -> deterministic synthetic vault generation (default `vault/generated`).
- `bun run tao:dev -- --json vault open --vault-root <path>` -> CLI open/bootstrap using vault-root only.
- `swift run --package-path apps/tao-macos TaoMacOSApp` -> launch macOS app.

## 5. Architecture
- SDK-first layering:
  - Domain and persistence live in Rust SDK crates.
  - CLI is a minimal adapter over SDK service calls.
  - macOS app is a native UI shell that consumes generated bindings.
- Runtime bootstrap:
  - Config precedence: defaults < root `config.toml` < vault `config.toml` < env < explicit overrides.
  - Vault-only startup path is supported; sqlite path auto-resolves and bootstraps.
- Bridge strategy:
  - `tao-sdk-bridge` exposes `TaoBridgeRuntime` with persistent in-process handle semantics.
  - Swift client caches runtime handles per `(vault_root, db_path)` key to avoid per-call process startup.
  - Core UI calls use batched/windowed methods (`startup_bundle_json`, `notes_window_json`, `note_context_json`) to reduce boundary overhead.

## 6. UI Behavior
- Sidebar: single hierarchical file/folder tree built from indexed note summaries.
- Detail pane: note title, parsed front matter properties, rendered markdown body.
- Settings: vault path only; selecting/saving vault triggers data refresh.
- Loading policy: note spinner is delayed to avoid visible flicker on fast reads.
- Debug/diagnostic panes are intentionally removed from primary app surface.

## 7. Performance and Benchmarks
- Benchmark output root: `.benchmarks/reports/` (gitignored).
- Guardrails:
  - Bridge scenario: `note_get`, `notes_list`, `note_put`, `events_poll`.
  - FFI scenario: `note_open`, `tree_window`, `startup_bundle`.
  - Startup scenario: end-to-end open/list/context budget.
- Budget gate script fails on latency regressions and writes machine-readable JSON reports.

## 8. Quality Gates
- Required before completion:
  - zero formatting drift (`cargo fmt --all`)
  - zero clippy warnings (`-D warnings`)
  - passing release tests (`cargo test --workspace --release`, `swift test --configuration release`)
  - successful release build (`bun run util:build`)
- Commit policy:
  - signed commits (`git commit -S`)
  - scoped Conventional Commit messages
  - atomic commits aligned to ticket boundaries

## 9. Roadmap State
- Control plane lives in `plan/`.
- Current phase family:
  - completed foundation: docs, architecture, sdk, db, indexing, link/property/base, cli baseline
  - completed transition: rename to `tao`, config bootstrap, vault-root CLI auto-init
  - active transition: native UniFFI bridge adoption, simplified production app shell, repository hygiene
- Execution contract:
  - update `plan/tickets.csv`, `plan/run-state.json`, and `plan/progress.md` with evidence on each completed ticket
  - keep blockers explicit in `plan/blockers.md`
