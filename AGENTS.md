> Tao is a local, Rust-native knowledge engine for markdown vaults, designed for deterministic automation and AI agents, with a thin SwiftUI desktop client

## 1. Documentation

- Primary policy: this file is the canonical operational reference for repository architecture, commands, quality gates, and roadmap state.
- Embedded spec decisions are maintained here (no standalone `docs/` spec directory):
    - CLI IA and removed-legacy-command policy
    - unified query contract and scope semantics
    - parity scope (`now/next/later`)
    - synthetic fixture and performance benchmark runbooks
- External references for tool behavior and standards:
    - `https://bun.sh/docs/llms.txt`
    - `https://swift.org/documentation/`
    - `https://www.rust-lang.org/learn`
    - `https://mozilla.github.io/uniffi-rs/latest/`
- Internal source references:
    - Workspace manifest: `Cargo.toml`
    - Script/runtime manifest: `package.json`
    - CLI guide: `crates/tao-cli/README.md`
    - SDK service guide: `crates/tao-sdk-service/README.md`
    - macOS package manifest: `apps/tao-macos/Package.swift`
    - Benchmark driver: `scripts/bench.sh`
    - Fixture driver: `scripts/fixtures.sh`
    - Safety scan: `scripts/safety.sh`

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
scripts/                        Operational scripts (clean, ffi, fixtures, bench, release)
.github/workflows/              Central quality-gate workflows
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
- `bun run dev -- vault open --vault-root <path>` -> CLI open/bootstrap using vault-root only.
- `swift run --package-path apps/tao-macos TaoMacOSApp` -> launch macOS app.

## 5. Architecture

- SDK-first layering:
    - Domain and persistence live in Rust SDK crates.
    - CLI is a minimal adapter over SDK service calls.
    - macOS app is a native UI shell that consumes generated bindings.
- Runtime bootstrap:
    - Config precedence: defaults < global `~/.tools/tao/config.toml` < root `config.toml` < vault `config.toml` < env < explicit overrides.
    - Root config is probe-only (loaded when present) and is not auto-created outside repository contexts.
    - Vault-only startup path is supported; sqlite path auto-resolves and bootstraps.
    - CLI `--vault-root` is optional; default vault can be set via `~/.tools/tao/config.toml` `[vault].root`.
- Bridge strategy:
    - `tao-sdk-bridge` exposes `TaoBridgeRuntime` with persistent in-process handle semantics.
    - Swift client caches runtime handles per `(vault_root, db_path)` key to avoid per-call process startup.
    - Core UI calls use batched/windowed methods (`startup_bundle_json`, `notes_window_json`, `note_context_json`) to reduce boundary overhead.

## 6. UI Behavior

- Sidebar: single hierarchical file/folder tree built from indexed note summaries.
- Detail pane: note title, bridge-provided structured front matter properties, rendered markdown body.
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
    - `docs` query coverage includes simple, `--where`, `--sort`, and combined projection/filter/sort variants.
- `bench:cli` writes per-command `hyperfine` JSON reports and `.benchmarks/reports/cli-readonly/summary.json`.

## 8. Quality Gates

- Required before completion:
    - zero formatting drift (`cargo fmt --all`)
    - zero clippy warnings (`-D warnings`)
    - passing release tests (`cargo test --workspace --release`, `swift test --configuration release`)
    - successful release build (`bun run build`)
- Local enforcement:
    - `bun run util:check` remains the full macOS-first developer gate.
    - Husky + lint-staged run `bun run util:check` before local commits.
- Central enforcement:
    - `.github/workflows/quality.yml` runs safety, Biome, format, lint, Rust typecheck/tests, and audit on Ubuntu.
    - `.github/workflows/apple.yml` runs FFI generation, release Swift tests, and release build on macOS.
- Commit policy:
    - signed commits (`git commit -S`)
    - scoped Conventional Commit messages
    - atomic commits aligned to ticket boundaries

## 9. Roadmap State

- No tracked `plan/` control-plane directory exists in the repository.
- Treat current git history, benchmark reports under `.benchmarks/reports/`, and crate/app test suites as the execution record.
- Active improvement areas reflected in source and benchmarks:
    - query correctness and projection parity
    - bridge/app data-shape parity
    - benchmark coverage for expensive query paths
    - release and CI hygiene

## 10. CLI IA and Contract

- Compact command surface:
    - `tools`, `health`, `vault`, `doc`, `base`, `graph`, `meta`, `task`, `query`
- Legacy aliases are removed from the public CLI:
    - `note`, `links`, `properties`, `bases`, `search` must return unknown-command errors
- JSON envelope contract for all non-interactive commands by default:
    - success: `{ ok: true, data: <payload>, meta: { tool, elapsed, count?, total?, hasMore? } }`
    - failure: `{ ok: false, error: { code, message, hint, details? }, meta: { tool, elapsed } }`
    - no `--json` flag exists; JSON is the default non-interactive contract
    - `--text` is the explicit opt-out for plain-text summaries.
    - bare `tao` and `tao --help` render clap help, not JSON.
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
    - frontmatter wikilinks + body wikilinks/markdown-links/embeds indexing parity
    - scoped inbound attachment/file audits via `graph inbound-scope`
    - graph diagnostics/traversal (`unresolved`, `deadends`, `orphans`, `floating`, `components`, `walk`)
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
