> `tao` is a Rust-first knowledge engine for markdown vaults: a JSON-first CLI over SDK services, an internal bridge adapter for daemon/runtime flows, and deterministic fixture and benchmark tooling.

## 1. Documentation

- Primary references: [`Cargo.toml`](Cargo.toml), [`package.json`](package.json), [`config.toml`](config.toml), [`crates/tao-cli/README.md`](crates/tao-cli/README.md), [`crates/tao-sdk-service/README.md`](crates/tao-sdk-service/README.md), [`crates/tao-sdk-bridge/README.md`](crates/tao-sdk-bridge/README.md), [`crates/tao-bench/README.md`](crates/tao-bench/README.md)
- Operational scripts are authoritative for release, safety, fixtures, and benchmarks: [`scripts/release.sh`](scripts/release.sh), [`scripts/safety.sh`](scripts/safety.sh), [`scripts/fixtures.sh`](scripts/fixtures.sh), [`scripts/bench.sh`](scripts/bench.sh), [`scripts/budgets.sh`](scripts/budgets.sh)
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
├── scripts/                   Safety, fixtures, benchmarks, release, cleanup
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
| CLI | `clap` + JSON envelopes | default JSON output, optional daemon forwarding, `--text` opt-out |
| Native bridge | `tao-sdk-bridge` | internal Rust adapter shared by CLI warm-runtime flows and bridge benchmarks |
| Tooling | Bun + Husky + Biome | JS tooling only; core product/runtime is Rust |
| Benchmarks | `tao-bench` + `hyperfine` | timestamped reports under [`.benchmarks/reports/`](.benchmarks/reports/) |

## 4. Commands

- `bun install` installs JS tooling and activates Husky hooks
- `cargo run -p tao-cli -- --help` iterates on the CLI without requiring a prior release build
- `bun run util:check` is the full completion gate: safety scan, Biome, `cargo fmt`, clippy, release `cargo check`, release tests, `cargo audit`, and `bun run build`
- `bun run build` packages release CLI artifacts via [`scripts/release.sh`](scripts/release.sh)
- `bun run bench`, `bun run bench:sdk`, `bun run bench:cli`, and `bun run bench:budget` run the benchmark and read-budget suites against repository-local fixtures
- `./scripts/fixtures.sh --profile parity` refreshes the compact parity fixtures; `./scripts/fixtures.sh --profile 10k` generates the larger benchmark vaults

## 5. Architecture

- [`crates/tao-cli/src/cli_impl/commands/`](crates/tao-cli/src/cli_impl/commands/) is an adapter layer only; keep business rules in SDK crates and keep envelope/CLI formatting out of service code
- [`crates/tao-sdk-service/src/`](crates/tao-sdk-service/src/) orchestrates indexing, reconcile, graph diagnostics, base execution, task/property operations, and health snapshots over storage and vault primitives
- [`crates/tao-sdk-storage/src/`](crates/tao-sdk-storage/src/) owns SQLite migrations, repositories, and transaction helpers
- [`crates/tao-sdk-vault/src/`](crates/tao-sdk-vault/src/) enforces vault boundaries and deterministic scan/fingerprint behavior; scans skip `.git`, `.obsidian`, and `.tao`
- [`crates/tao-sdk-bridge/src/`](crates/tao-sdk-bridge/src/) exposes `BridgeKernel` and envelope types used by CLI runtime caches and retained benchmark flows
- `vault reindex` is not a blind full rebuild: it prefers incremental reconcile and only escalates to full rebuild when link-resolution version state or indexed file-path consistency is stale

## 6. Runtime and State

- Vault root resolution is separate from other settings: `--vault-root` -> `TAO_VAULT_ROOT` -> `[vault].root` from repo/root `config.toml` discovered from cwd -> `[vault].root` from global `~/.tools/tao/config.toml`; once the vault is known, runtime/storage/security values resolve as explicit overrides -> `TAO_*` env vars -> vault `config.toml` -> repo/root config -> global config -> built-in defaults
- Relevant env vars: `TAO_VAULT_ROOT`, `TAO_CONFIG_PATH`, `TAO_DATA_DIR`, `TAO_DB_PATH`, `TAO_CASE_POLICY`, `TAO_TRACING_ENABLED`, `TAO_FEATURE_FLAGS`, `TAO_READ_ONLY`; `TAO_CONFIG_PATH` overrides the global config file location, and release/cleanup also honor `TAO_HOME`, `TOOLS_HOME`, and legacy `OBS_HOME`
- Probe-only config behavior is intentional: root and vault `config.toml` files are read when present but are not auto-created during normal config resolution
- Effective runtime defaults when config is absent are repo-local or vault-local: data dir `<vault>/.tao`, db path `<vault>/.tao/index.sqlite`, case-sensitive matching, tracing enabled, read-only enabled
- Normal vault-facing CLI commands may auto-forward through a background daemon; `vault daemon *` is the explicit lifecycle/inspection surface, not the sole entrypoint to warm-runtime mode
- Daemon sockets are Unix-only and default to `~/.tools/tao/daemons/vault-<hash>.sock`; when `HOME` is missing the fallback is `<cwd>/.tao/daemons/`
- Daemon first observation may reconcile or fully rebuild before serving cached reads; later change-monitor generations invalidate cached results for the affected runtime
- Generated and local state to expect: [`dist/`](dist/), [`.benchmarks/reports/`](.benchmarks/reports/), [`vault/generated/`](vault/generated/), and local vault metadata directories like `vault/.tao/`
- [`scripts/budgets.sh`](scripts/budgets.sh) optionally reads `plan/perf-budgets.json`, but that file is absent in the current repo; the script falls back to `profile=10k` and `10ms` default p50 budgets

## 7. Conventions

- `README.md` and `CLAUDE.md` are symlink mirrors of [`AGENTS.md`](AGENTS.md); edit the root file only
- Non-interactive CLI commands emit one JSON envelope to stdout by default; bare `tao` and help/version flows use native clap output, and `--text` is the explicit opt-out
- `--json-stream` is a narrow fast path: it only applies to `query --from docs` without `--where` or `--sort`
- `query --from graph` without `--path` maps to the unresolved-link window; with `--path` it returns outgoing and backlink panels
- Mutations are gated in both CLI and bridge layers: CLI `doc write` and `task set-state` require `--allow-writes` unless `[security].read_only = false`, while bridge note writes still require explicit write enablement unless the same config disables read-only mode
- If you change command names, parameters, or examples, update [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs) and the contract tests that assert the public surface

## 8. Constraints

- Never run automated QA, fixture generation, or benchmarks against personal vaults or paths outside this repository; [`scripts/safety.sh`](scripts/safety.sh) enforces repository-local paths and blocks Dropbox roots
- Prefer [`vault/`](vault/), [`vault/generated/`](vault/generated/), or repo-local temporary directories for all test and benchmark vaults
- Treat [`crates/tao-sdk-storage/`](crates/tao-sdk-storage/), [`crates/tao-sdk-bridge/`](crates/tao-sdk-bridge/), [`crates/tao-cli/src/cli_impl/contract.rs`](crates/tao-cli/src/cli_impl/contract.rs), [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs), and [`scripts/`](scripts/) as high-risk boundaries for migrations, contract stability, packaging, and safety
- [`scripts/clean.sh`](scripts/clean.sh) removes `dist`, `TAO_HOME`, and the legacy `${OBS_HOME:-${TOOLS_HOME}/obs}` install directory; do not run it casually if those env vars point somewhere unexpected
- CLI/daemon/budget benchmark flows assume repository-local generated fixtures; daemon and budget suites additionally require Unix sockets and `hyperfine`, while raw `tao-bench` scenarios (`bridge`, `startup`, `parse`, `resolve`, `search`, `graph-walk`, `unified-query`) do not

## 9. Validation

- Required gate: `bun run util:check`
- CLI and JSON contract changes: `cargo test -p tao-cli --release`
- Service, bridge, or indexing changes: `cargo test -p tao-sdk-service --release` and `cargo test -p tao-sdk-bridge --release`
- Fixture or graph/base parity changes: use [`vault/fixtures/README.md`](vault/fixtures/README.md), rerun the parity refresh flow, and keep [`vault/fixtures/graph-parity/expected/`](vault/fixtures/graph-parity/expected/) in sync with CLI snapshot tests
- Benchmark or performance changes: rerun the relevant suites from [`scripts/bench.sh`](scripts/bench.sh) and [`scripts/budgets.sh`](scripts/budgets.sh); reports land under [`.benchmarks/reports/`](.benchmarks/reports/) with a `latest` symlink
- There is no tracked CI workflow directory at the repo root today, so local script/test output is the completion bar

## 10. Further Reading

- [`scripts/tests/safety_test.sh`](scripts/tests/safety_test.sh) for the hard path-safety expectations the repo actively tests
