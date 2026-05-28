> `tao` is a Rust-first knowledge engine for markdown vaults: a JSON-first CLI over SDK services, an internal bridge adapter for daemon/runtime flows, and deterministic fixture and benchmark tooling.

## 1. Documentation

- Primary references: [`Cargo.toml`](Cargo.toml), [`package.json`](package.json), [`config.toml`](config.toml), [`crates/tao-cli/README.md`](crates/tao-cli/README.md), [`crates/tao-sdk-service/README.md`](crates/tao-sdk-service/README.md), [`crates/tao-sdk-bridge/README.md`](crates/tao-sdk-bridge/README.md), [`crates/tao-bench/README.md`](crates/tao-bench/README.md)
- Operational scripts are authoritative for release, path guards, fixtures, and benchmarks: [`scripts/release.sh`](scripts/release.sh), [`scripts/path-guards.sh`](scripts/path-guards.sh), [`scripts/fixtures.sh`](scripts/fixtures.sh), [`scripts/bench.sh`](scripts/bench.sh), [`scripts/budgets.sh`](scripts/budgets.sh)
- Fixture semantics live in [`vault/README.md`](vault/README.md) and [`vault/fixtures/README.md`](vault/fixtures/README.md)
- External docs used by this repo: [Rust](https://www.rust-lang.org/learn), [Bun](https://bun.sh/docs/llms.txt)
- There is no tracked `.github/workflows/` directory in the current repository; treat local scripts, hooks, and crate tests as the real enforcement surface

## 2. Repository Structure

```text
.
├── crates/
│   ├── tao-cli/               JSON-first CLI surface, daemon client/server, contract tests
│   ├── tao-sdk-*/             Core SDK crates: config, vault scan, storage, service, internal bridge, search
│   ├── tao-bench/             Deterministic benchmark harness
│   └── tao-tui/               Placeholder TUI shell
├── scripts/                   Path guards, fixtures, benchmarks, release, cleanup
├── vault/                     Shipped QA/conformance fixture vault plus parity fixtures
└── AGENTS.md                  Canonical repo-level agent instructions; `README.md` and `CLAUDE.md` symlink here
```

- Start behavior changes in [`crates/tao-cli/src/cli_impl/commands/`](crates/tao-cli/src/cli_impl/commands/) for CLI routing, [`crates/tao-sdk-service/src/`](crates/tao-sdk-service/src/) for orchestration, and [`crates/tao-sdk-storage/src/`](crates/tao-sdk-storage/src/) for SQLite schema/repository work
- [`vault/fixtures/graph-parity/expected/`](vault/fixtures/graph-parity/expected/) holds golden JSON snapshots for CLI graph contracts
- [`dist/`](dist/), [`.benchmarks/`](.benchmarks/), [`target/`](target/), and [`vault/generated/`](vault/generated/) are generated runtime/build outputs

## 3. Stack

| Layer | Choice | Notes |
| --- | --- | --- |
| Core engine | Rust 2024 workspace | `unsafe_code = "forbid"` at workspace level, strict clippy |
| Storage | SQLite via `rusqlite` | schema and migrations owned by `tao-sdk-storage` |
| Vault FS | `tao-sdk-vault` | canonical path safety, NFC normalization, case-policy matching |
| CLI | `clap` + JSON envelopes | default JSON output, optional daemon forwarding |
| Native bridge | `tao-sdk-bridge` | internal Rust adapter shared by CLI warm-runtime flows and bridge benchmarks |
| Tooling | Bun + Husky + Biome | JS tooling only; core product/runtime is Rust |
| Benchmarks | `tao-bench` + `hyperfine` | timestamped reports under [`.benchmarks/reports/`](.benchmarks/reports/) |

## 4. Commands

- `bun install` installs JS tooling and activates Husky hooks
- `cargo run -p tao-cli -- --help` iterates on the CLI without requiring a prior release build
- `bun run util:check` is the full completion gate: path-guard tests, Biome, `cargo fmt --check`, clippy, release `cargo check`, release tests, `cargo audit`, and `bun run build`
- `bun run build` packages release CLI artifacts via [`scripts/release.sh`](scripts/release.sh)
- `bun run bench`, `bun run bench:smoke`, and `bun run bench:budget` are the package benchmark entrypoints; pass suite flags through `bun run bench -- --suite live` or `bun run bench -- --suite cli`
- `./scripts/fixtures.sh --profile parity` refreshes compact parity fixtures; generated synthetic benchmark fixtures are limited to `1k` and `5k`
- `tao validate <path>` validates markdown frontmatter, `.base` files, or a non-recursive folder window; add `--recursive` for nested folders

## 5. Architecture

- [`crates/tao-cli/src/cli_impl/commands/`](crates/tao-cli/src/cli_impl/commands/) is an adapter layer only; keep business rules in SDK crates and keep envelope/CLI formatting out of service code
- [`crates/tao-sdk-service/src/`](crates/tao-sdk-service/src/) orchestrates indexing, reconcile, graph diagnostics, base execution, task/property operations, and health snapshots over storage and vault primitives
- [`crates/tao-sdk-storage/src/`](crates/tao-sdk-storage/src/) owns SQLite migrations, repositories, and transaction helpers
- [`crates/tao-sdk-vault/src/`](crates/tao-sdk-vault/src/) enforces vault boundaries and deterministic scan/fingerprint behavior; scans skip `.git`, `.obsidian`, `.tao`, and root `.taoignore`, and honor root `.taoignore` patterns for Tao indexing exclusions without reading `.gitignore`
- [`crates/tao-sdk-bridge/src/`](crates/tao-sdk-bridge/src/) exposes `BridgeKernel` and envelope types used by CLI runtime caches and retained benchmark flows
- `vault reindex` is not a blind full rebuild: it prefers incremental reconcile and only escalates to full rebuild when link-resolution version state or indexed file-path consistency is stale
- `tao search` reads a derived unified search corpus (`search_segments`, `search_segments_fts`, `search_aliases`) built from the canonical file, doc FTS, property, task, graph, and base tables. `vault reindex`, incremental reconcile, daemon first-observation repair, and one-shot search stale checks keep that corpus in sync with the core index.
- Public graph help is centered on `graph links`, `graph audit`, `graph path`, and `graph walk`; older graph-specific subcommands remain callable as compatibility wrappers, are omitted from default `tao tools`, and can still be inspected with `tao tools <name>`

## 6. Runtime and State

- Vault root resolution is separate from other settings: `--vault-root` -> `TAO_VAULT_ROOT` -> `[vault].root` from repo/root `config.toml` discovered from cwd -> `[vault].root` from global `~/.tools/tao/config.toml`; once the vault is known, runtime/storage/security values resolve as explicit overrides -> `TAO_*` env vars -> vault `config.toml` -> repo/root config -> global config -> built-in defaults
- Relevant env vars: `TAO_VAULT_ROOT`, `TAO_CONFIG_PATH`, `TAO_DATA_DIR`, `TAO_DB_PATH`, `TAO_CASE_POLICY`, `TAO_TRACING_ENABLED`, `TAO_FEATURE_FLAGS`, `TAO_READ_ONLY`; `TAO_CONFIG_PATH` overrides the global config file location, and release/cleanup also honor `TAO_HOME`, `TOOLS_HOME`, and legacy `OBS_HOME`
- Probe-only config behavior is intentional: root and vault `config.toml` files are read when present but are not auto-created during normal config resolution
- Effective runtime defaults when config is absent are repo-local or vault-local: data dir `<vault>/.tao`, db path `<vault>/.tao/index.sqlite`, case-sensitive matching, tracing enabled, read-only enabled
- `config show` reports effective config values, per-field source labels, source inputs, and precedence without opening or migrating SQLite state
- Normal vault-facing CLI commands may auto-forward through a background daemon; hidden `vault daemon *` commands remain lifecycle/inspection escape hatches, not the normal user workflow
- Daemon sockets are Unix-only and default to `~/.tools/tao/daemons/vault-<hash>.sock`; when `HOME` is missing the fallback is `<cwd>/.tao/daemons/`
- Daemon first observation may reconcile or fully rebuild before serving cached reads; later change-monitor generations invalidate cached results for the affected runtime
- Generated and local state to expect: [`dist/`](dist/), [`.benchmarks/reports/`](.benchmarks/reports/), [`vault/generated/`](vault/generated/), and local vault metadata directories like `vault/.tao/`
- [`scripts/budgets.sh`](scripts/budgets.sh) optionally reads `plan/perf-budgets.json`, but that file is absent in the current repo; the script falls back to `profile=5k` and `10ms` default p50 budgets

## 7. Conventions

- `README.md` and `CLAUDE.md` are symlink mirrors of [`AGENTS.md`](AGENTS.md); edit the root file only
- Non-interactive CLI commands emit one JSON envelope to stdout by default; bare `tao` and help/version flows use native clap output.
- `--json-stream` is a narrow projected JSON envelope path: it only applies to `query --from docs` without `--where` or `--sort`
- `query --from graph` without `--path` maps to the unresolved-link window; with `--path` it returns outgoing and backlink panels
- Public vault-content operations are read-only. `doc write`, `task set-state`, global `--allow-writes`, and public `--text` output are not part of the CLI surface.
- Internal state writes for `vault open`, `vault reindex`, daemon/cache/index maintenance, watch reconciliation, search-corpus repair, and health synchronization remain allowed; `vault reindex --dry-run` inspects planned index work.
- `tao search <query>` is the primary graph-aware exploration entrypoint across indexed markdown docs, the indexed file inventory, bases/frontmatter properties, tasks, graph links, and context expansion. Use `rg` for raw grep; use `tao search` when index metadata, exact aliases, normalized spaces/underscores/hyphens, canonical ranking, and relationships matter.
- `tao validate <path>` is the public validation surface for markdown frontmatter and `.base` files; `tao base validate` is not part of the public command surface.
- If you change command names, parameters, or examples, update [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs) and the contract tests that assert the public surface

## 8. Constraints

- Do not run general automated QA or fixture generation against personal vaults or paths outside this repository. Use [`vault/`](vault/), [`vault/generated/`](vault/generated/), or repo-local temporary directories for tests and generated fixtures.
- Live-vault smoke checks and live-vault benchmarks are allowed because the public CLI is vault-content read-only. Pass live paths at runtime with `--live-vault` or `TAO_BENCH_LIVE_VAULT`; keep private benchmark probes in gitignored `.benchmarks/live-commands.txt`, never in tracked files.
- Treat [`crates/tao-sdk-storage/`](crates/tao-sdk-storage/), [`crates/tao-sdk-bridge/`](crates/tao-sdk-bridge/), [`crates/tao-cli/src/cli_impl/contract.rs`](crates/tao-cli/src/cli_impl/contract.rs), [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs), and [`scripts/`](scripts/) as high-risk boundaries for migrations, contract stability, packaging, and path/output guardrails
- [`scripts/clean.sh`](scripts/clean.sh) removes `dist`, `TAO_HOME`, and the legacy `${OBS_HOME:-${TOOLS_HOME}/obs}` install directory; do not run it casually if those env vars point somewhere unexpected
- CLI/daemon/budget benchmark flows use repository-local generated fixtures by default; `bun run bench -- --suite live` uses a runtime-provided live vault. Daemon, live, fixture-generation, and budget suites require Unix sockets and `hyperfine`, while raw `tao-bench` scenarios (`bridge`, `startup`, `parse`, `resolve`, `search`, `graph-walk`, `unified-query`) do not.

## 9. Validation

- Required gate: `bun run util:check`
- CLI and JSON contract changes: `cargo test -p tao-cli --release`
- Service, bridge, or indexing changes: `cargo test -p tao-sdk-service --release` and `cargo test -p tao-sdk-bridge --release`
- Fixture or graph/base parity changes: use [`vault/fixtures/README.md`](vault/fixtures/README.md), rerun the parity refresh flow, and keep [`vault/fixtures/graph-parity/expected/`](vault/fixtures/graph-parity/expected/) in sync with CLI snapshot tests
- Benchmark or performance changes: rerun the relevant suites from [`scripts/bench.sh`](scripts/bench.sh) and [`scripts/budgets.sh`](scripts/budgets.sh); reports land under [`.benchmarks/reports/`](.benchmarks/reports/) with a `latest` symlink
- There is no tracked CI workflow directory at the repo root today, so local script/test output is the completion bar

## 10. Further Reading

- [`scripts/tests/path_guards_test.sh`](scripts/tests/path_guards_test.sh) for the generic repository-local output and live-vault path guard expectations the repo actively tests
