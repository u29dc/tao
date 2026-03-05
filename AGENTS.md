> Tao is a local, Rust-native knowledge engine for markdown vaults, designed for deterministic automation and AI agents, with a thin SwiftUI desktop client

## 1. Documentation

- Primary policy: this file is the canonical operational reference for repository architecture, commands, quality gates, and roadmap state.
- Embedded spec decisions are maintained here (no standalone `docs/` spec directory):
    - CLI IA and compatibility alias map
    - unified query contract and scope semantics
    - parity scope (`now/next/later`)
    - synthetic fixture and performance benchmark runbooks
- External references for tool behavior and standards:
    - `https://bun.sh/docs/llms.txt`
    - `https://swift.org/documentation/`
    - `https://www.rust-lang.org/learn`
    - `https://mozilla.github.io/uniffi-rs/latest/`
- Internal source references:
    - Phase guide and roadmap: `plan/README.md`
    - Ticket ledger: `plan/tickets.csv`
    - Run state: `plan/run-state.json`
    - Progress log: `plan/progress.md`
    - Execution checklist: `plan/checklists/execution.md`
    - Review checklist: `plan/checklists/review.md`
    - Crate README template: `plan/templates/crate-readme-template.md`
    - Architecture map: `plan/architecture-map.md`

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
scripts/                        Operational scripts (clean, ffi, fixtures, bench, release)
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
- `bun run util:safety` -> enforce repository-local safety scan (forbidden personal-path markers).
- `bun run build` -> full release build pipeline (`util:ffi` + `release:cli` + `release:mac`).
- `bun run release:all` -> unified release pipeline for CLI + macOS app packaging.
- `bun run release:cli` -> release Rust binaries install + CLI/TUI bundle output.
- `bun run release:mac` -> release Swift app bundle + signed zip package output.
- `bun run util:check` -> format, lint, release check/test, audit, and full release build.
- `bun run bench` / `bun run bench:all` -> full benchmark suite (`sdk` + read-only CLI matrix).
- `bun run bench:sdk` -> SDK/bridge/startup scenarios + baseline query/graph budgets.
- `bun run bench:cli` -> comprehensive read-only CLI benchmark matrix.
- `bun run bench:daemon` -> one-shot vs daemon query latency comparison with improvement gate.
- `./scripts/bench.sh` -> unified benchmark driver (`--suite all|sdk|cli|daemon|bridge|ffi|startup|parse|resolve|search`).
- `./scripts/fixtures.sh [output-root]` -> deterministic synthetic vault generation (default `vault/generated`).
- `./scripts/fixtures.sh --skip-validate` -> opt out of built-in fixture validation.
- `bun run dev -- --json vault open --vault-root <path>` -> CLI open/bootstrap using vault-root only.
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
- Core guardrails (`bench:sdk`):
    - Bridge scenario: `note_get`, `notes_list`, `note_put`, `events_poll`.
    - FFI scenario: `note_open`, `tree_window`, `startup_bundle`.
    - Startup scenario: end-to-end open/list/context budget.
- Baseline CLI latency budgets (enforced in `bench:sdk`):
    - `query --from docs` mean <= `10ms`
    - `graph unresolved` mean <= `10ms`
- Comprehensive CLI matrix (`bench:cli`) benchmarks all read-only command families:
    - `vault` read paths, `doc` read/list, `base` list/view/schema, `graph` diagnostics/traversal, `meta`, `task list`, and `query` scopes.
- `bench:cli` writes per-command `hyperfine` JSON reports and `.benchmarks/reports/cli-readonly/summary.json`.

## 8. Quality Gates

- Required before completion:
    - zero formatting drift (`cargo fmt --all`)
    - zero clippy warnings (`-D warnings`)
    - passing release tests (`cargo test --workspace --release`, `swift test --configuration release`)
    - successful release build (`bun run build`)
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

## 10. CLI IA and Compatibility Contract

- Compact command surface:
    - `vault`, `doc`, `base`, `graph`, `meta`, `task`, `query`
- Compatibility aliases remain supported:
    - `note`, `links`, `properties`, `bases`, `search`
- JSON envelope contract for all `--json` commands:
    - `{ ok, value: { command, summary, args }, error }`
    - failures return `ok=false`, `value=null`, structured `error`.
- Write gate policy:
    - mutating operations require `--allow-writes`
    - read-only operations must never require write gate.

## 11. Unified Query Contract

- Entrypoint:
    - `tao query --vault-root <path> --from <scope> [options]`
- Supported scopes:
    - `docs`
    - `graph` (unresolved by default; outgoing/backlinks when `--path` is supplied)
    - `task`
    - `meta:tags`
    - `meta:aliases`
    - `meta:properties`
    - `base:<id-or-path>` (requires `--view-name`)
- Core query options:
    - `--query <text>`
    - `--path <note-path>`
    - `--view-name <name>`
    - `--limit <n>`
    - `--offset <n>`

## 12. Parity Scope Map

- Now:
    - compact CLI IA
    - frontmatter + body wikilink indexing parity
    - graph diagnostics/traversal (`unresolved`, `deadends`, `orphans`, `components`, `walk`)
    - metadata aggregations (`properties`, `tags`, `aliases`, `tasks`)
    - task extraction + state transitions with write gate
    - deterministic synthetic fixture generation + validation
- Next:
    - planner-level projection/ranking/explain parity for all query adapters
    - richer relation-aware base typing and schema introspection
    - persistent daemon runtime (`taod`) and warm client mode
    - incremental reindex + hot-query cache budget gates
- Later:
    - sync/recovery/versioned retention flows
    - app-shell control parity surfaces
    - advanced task workflows (priority, recurrence, assignees, rollups)

## 13. Synthetic Fixture Policy

- Fixture generator:
    - `./scripts/fixtures.sh --profile all --seed 42 --output vault/generated`
    - profiles: `1k`, `2k`, `5k`, `10k`, `25k`, `all`
- Validation:
    - built into `./scripts/fixtures.sh` by default
- Validation invariants:
    - no hub files
    - required base files (`contacts`, `companies`, `projects`, `meetings`)
    - markdown tasks/tags present
    - body + frontmatter wikilinks present
    - unresolved ratio within bounded range
    - no personal vault/path leakage markers

## 14. Hard Safety Rule (Non-Negotiable)

- Never access real personal vaults or personal folders for development, benchmarking, or QA.
- Forbidden paths include all Dropbox/personal roots, especially:
    - `/Users/han/Library/CloudStorage/Dropbox/**`
    - `/Users/han/Dropbox/**`
    - any path outside this repository root for automated test/bench workflows
- Allowed vault roots for all automated work:
    - `vault/`
    - `vault/generated/**`
    - temporary directories created under this repository root only
- Enforcement requirements:
    - benchmark scripts must generate/use repository-local fixtures only
    - fixtures validation must fail on leaked personal-path markers
    - CI/quality checks must include forbidden-path scanning
- Violation policy:
    - treat any access attempt to forbidden paths as a hard failure and stop execution.
