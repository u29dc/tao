> `tao` is a Rust-first knowledge engine for markdown vaults: a JSON-first CLI over SDK services, an internal bridge adapter for daemon/runtime flows, and deterministic fixture and benchmark tooling.

## 1. Documentation

- Primary references: [`Cargo.toml`](Cargo.toml), [`package.json`](package.json), [`config.toml`](config.toml), [`crates/tao-cli/README.md`](crates/tao-cli/README.md), [`crates/tao-sdk-service/README.md`](crates/tao-sdk-service/README.md), [`crates/tao-sdk-bridge/README.md`](crates/tao-sdk-bridge/README.md), [`crates/tao-bench/README.md`](crates/tao-bench/README.md)
- Tooling entrypoints live in [`package.json`](package.json); shared output guards are in [`scripts/support.py`](scripts/support.py)
- Fixture semantics and golden-update procedures live in [`fixtures/README.md`](fixtures/README.md)
- External docs used by this repo: [Rust](https://www.rust-lang.org/learn), [Bun](https://bun.sh/docs/llms.txt)
- There is no tracked `.github/workflows/` directory in the current repository; treat local scripts, hooks, and crate tests as the real enforcement surface

## 2. Repository Structure

```text
.
├── crates/
│   ├── tao-cli/               JSON-first CLI surface, daemon client/server, contract tests
│   ├── tao-sdk-*/             Core SDK crates: config, vault scan, storage, service, internal bridge, search
│   └── tao-bench/             Deterministic benchmark harness
├── scripts/                   Path guards, fixtures, benchmarks, release, cleanup
├── fixtures/                  Tracked conformance/base/graph vaults and golden expectations
└── AGENTS.md                  Canonical repo-level agent instructions; `README.md` and `CLAUDE.md` symlink here
```

- Start behavior changes in [`crates/tao-cli/src/cli_impl/commands/`](crates/tao-cli/src/cli_impl/commands/) for CLI routing, [`crates/tao-sdk-service/src/`](crates/tao-sdk-service/src/) for orchestration, and [`crates/tao-sdk-storage/src/`](crates/tao-sdk-storage/src/) for SQLite schema/repository work
- [`fixtures/graph/expected/`](fixtures/graph/expected/) holds golden JSON snapshots for CLI graph contracts
- `dist/`, [`.benchmarks/`](.benchmarks/), [`target/`](target/), and [`target/fixtures/`](target/fixtures/) are generated runtime/build outputs

## 3. Stack

| Layer | Choice | Notes |
| --- | --- | --- |
| Core engine | Rust 2024 workspace | `unsafe_code = "forbid"` at workspace level, strict clippy |
| Storage | SQLite via `rusqlite` | current schema and format validation owned by `tao-sdk-storage` |
| Vault FS | `tao-sdk-vault` | canonical path safety, NFC normalization, case-policy matching |
| CLI | `clap` + JSON/Toon envelopes | default JSON output, optional `--toon`, optional daemon forwarding |
| Native bridge | `tao-sdk-bridge` | internal Rust adapter shared by CLI warm-runtime flows and bridge benchmarks |
| Tooling | Bun, Python 3, Husky, Biome | Development/release tooling only; core product/runtime is Rust |
| Benchmarks | `tao-bench` + Python process driver | timestamped reports under [`.benchmarks/reports/`](.benchmarks/reports/) |

## 4. Commands

- `bun install` installs JS tooling and activates Husky hooks
- `cargo run -p tao-cli -- --help` iterates on the CLI without requiring a prior release build
- `bun run util:check` is the full completion gate: tooling/path-safety tests, Biome, `cargo fmt --check`, clippy, release `cargo check`, release tests, `cargo audit`, and `bun run build`
- `bun run build` builds and verifies the local CLI without installing; `bun run release:cli` packages and `bun run release:install` explicitly installs via [`scripts/release.py`](scripts/release.py)
- `bun run bench`, `bun run bench:smoke`, and `bun run bench:budget` are the package benchmark entrypoints; pass suite flags through `bun run bench -- --suite live` or `bun run bench -- --suite cli`
- `python3 -B scripts/fixtures.py --profile parity` refreshes compact parity fixtures; generated synthetic benchmark fixtures are limited to `1k` and `5k`
- `tao validate <path>` validates markdown frontmatter, `.base` files, or a non-recursive folder window; add `--recursive` for nested folders

## 5. Architecture

- [`crates/tao-cli/src/cli_impl/commands/`](crates/tao-cli/src/cli_impl/commands/) is an adapter layer only; keep business rules in SDK crates and keep envelope/CLI formatting out of service code
- [`crates/tao-sdk-service/src/`](crates/tao-sdk-service/src/) orchestrates indexing, reconcile, graph diagnostics, base execution, task/property operations, and health snapshots over storage and vault primitives
- [`crates/tao-sdk-storage/src/`](crates/tao-sdk-storage/src/) owns the current SQLite schema, format validation, repositories, and transaction helpers
- [`crates/tao-sdk-vault/src/`](crates/tao-sdk-vault/src/) enforces vault boundaries and deterministic scan/fingerprint behavior; scans skip `.git`, `.obsidian`, `.tao`, and root `.taoignore`, and honor root `.taoignore` patterns for Tao indexing exclusions without reading `.gitignore`
- [`crates/tao-sdk-bridge/src/`](crates/tao-sdk-bridge/src/) exposes `BridgeKernel` and envelope types used by CLI runtime caches and retained benchmark flows
- `vault reindex` is not a blind full rebuild: it prefers incremental reconcile and only escalates to full rebuild when link-resolution version state or indexed file-path consistency is stale
- TXT content and PDF extraction jobs use revision-bound content records. PDF workers use explicitly provisioned local Poppler and Tesseract tools; other assets retain inventory/group/path visibility without body extraction. `vault reindex --wait-content-ms <ms>` optionally drains pending PDF jobs.
- `tao search` reads a derived unified search corpus (`search_segments`, `search_segments_fts`, `search_aliases`) built from the canonical file, doc FTS, property, task, graph, and base tables. `vault reindex`, incremental reconcile, daemon first-observation repair, and one-shot search stale checks keep that corpus in sync with the core index.
- Graph commands are `graph links`, `graph audit`, `graph path`, and `graph walk`. Audit kinds select diagnostics within the existing command. Obsolete aliases are removed.

## 6. Runtime and State

- Vault root resolution is separate from other settings: `--vault-root` -> `TAO_VAULT_ROOT` -> `[vault].root` from repo/root `config.toml` discovered from cwd -> `[vault].root` from global `~/.tools/tao/config.toml`; once the vault is known, runtime/storage values resolve as explicit overrides -> `TAO_*` env vars -> vault `config.toml` -> repo/root config -> global config -> built-in defaults
- Relevant env vars: `TAO_VAULT_ROOT`, `TAO_CONFIG_PATH`, `TAO_DATA_DIR`, `TAO_DB_PATH`, `TAO_CASE_POLICY`; `TAO_CONFIG_PATH` overrides the global config file location, and explicit installation accepts `TAO_HOME` or an `--out` destination
- Probe-only config behavior is intentional: root and vault `config.toml` files are read when present but are not auto-created during normal config resolution
- Effective runtime defaults when config is absent are repo-local or vault-local: data dir `<vault>/.tao`, db path `<vault>/.tao/index.sqlite`, case-sensitive matching; public vault content operations are always read-only
- `config show` reports effective config values, per-field source labels, source inputs, and precedence without opening or migrating SQLite state
- `--execution-mode auto|direct|required-daemon` selects execution. `auto` may forward through a background daemon; `direct` never starts one. `--timeout-ms` bounds request execution. hidden `vault daemon *` commands remain lifecycle/inspection escape hatches, not the normal user workflow
- Daemon sockets are Unix-only and default to `~/.tools/tao/daemons/vault-<hash>.sock`; when `HOME` is missing the fallback is `<cwd>/.tao/daemons/`
- A missing daemon publication is prepared before reads. Existing publications remain readable while background reconciliation runs; `refreshPending` reports outstanding work. Explicit successful reindex acknowledges its observed watcher generation; cache identity follows committed publication generations. PDF subprocesses do not hold the runtime refresh lock.
- Generated and local state to expect: `dist/`, [`.benchmarks/reports/`](.benchmarks/reports/), [`target/fixtures/`](target/fixtures/), and local vault metadata directories inside disposable copies under `target/`
- [`scripts/bench.py --suite budget`](scripts/bench.py) validates workload-specific budget inputs and measured modes; missing or malformed budget results fail closed.

## 7. Conventions

- Top-level scripts use one lowercase word per filename. Python owns tooling orchestration; `cargo.sh` only selects the local toolchain. Bun commands call implementations directly, without paired shell wrappers.
- Tooling tests use `scripts/tests/test_<subsystem>.py`. Rust unit suites stay with their owning module; CLI adapter suites are grouped under `cli_impl/tests/`, and executable tests remain in crate `tests/` directories.
- Each tracked fixture family has a `vault/` directory and optional sibling `expected/` directory. Generated fixtures belong under `target/fixtures/`; benchmark reports and private probes belong under `.benchmarks/`.

- `README.md` and `CLAUDE.md` are symlink mirrors of [`AGENTS.md`](AGENTS.md); edit the root file only
- Non-interactive CLI commands emit one JSON envelope to stdout by default; bare `tao` and help/version flows use native clap output.
- `--toon` emits the normal public CLI envelope as Toon instead of default JSON.
- `--json-stream` is a narrow projected JSON envelope path: it only applies to `query --from docs` without `--where` or `--sort`, and remains JSON-only.
- `query --from graph` without `--path` maps to the unresolved-link window; with `--path` it returns outgoing and backlink panels
- Public vault-content operations are read-only. `doc write`, `task set-state`, global `--allow-writes`, and public `--text` output are not part of the CLI surface.
- Internal state writes for `vault open`, `vault reindex`, daemon/cache/index maintenance, watch reconciliation, search-corpus repair, remain allowed; `vault reindex --dry-run` inspects planned index work.
- `tao search <query>` is the primary graph-aware exploration entrypoint across indexed markdown docs, the indexed file inventory, bases/frontmatter properties, tasks, graph links, and context expansion. Use `rg` for raw grep; use `tao search` when index metadata, exact aliases, normalized spaces/underscores/hyphens, canonical ranking, and relationships matter.
- Current indexes use a single schema format epoch. Incompatible older indexes are refused with an explicit rebuild instruction; no historical migration chain or automatic source deletion is supported.
- `tao doc read --path <path>` returns a bounded indexed content window for Markdown/TXT/PDF or an asset inventory record. Continuation uses the returned `continuation_revision`; list/query docs remain Markdown-only.
- `tao validate <path>` is the public validation surface for markdown frontmatter and `.base` files; `tao base validate` is not part of the public command surface.
- If you change command names, parameters, or examples, update [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs) and the contract tests that assert the public surface

## 8. Constraints

- Do not run general automated QA or fixture generation against personal vaults or paths outside this repository. Copy source vaults from [`fixtures/`](fixtures/) into [`target/fixtures/`](target/fixtures/) or other repo-local temporary directories before indexing. Never write runtime state into tracked fixtures.
- Live-vault smoke checks and live-vault benchmarks are allowed because the public CLI is vault-content read-only. Pass live paths at runtime with `--live-vault` or `TAO_BENCH_LIVE_VAULT`; keep private benchmark probes in gitignored `.benchmarks/live-commands.txt`, never in tracked files.
- Treat [`crates/tao-sdk-storage/`](crates/tao-sdk-storage/), [`crates/tao-sdk-bridge/`](crates/tao-sdk-bridge/), [`crates/tao-cli/src/cli_impl/contract.rs`](crates/tao-cli/src/cli_impl/contract.rs), [`crates/tao-cli/src/cli_impl/registry.rs`](crates/tao-cli/src/cli_impl/registry.rs), and [`scripts/`](scripts/) as high-risk boundaries for migrations, contract stability, packaging, and path/output guardrails
- [`scripts/release.py clean`](scripts/release.py) removes only repository build outputs (`target`, `dist`); `--dry-run` previews them. Managed installation/uninstallation is explicit and preserves configuration, indexes, and runtime state.
- CLI/daemon/budget benchmark flows use repository-local generated fixtures by default; `bun run bench -- --suite live` uses a runtime-provided live vault. Daemon measurement modes require Unix sockets. Timing uses Python and the Rust harness without `hyperfine`.

## 9. Validation

- Required gate: `bun run util:check`
- CLI and JSON contract changes: `cargo test -p tao-cli --release`
- Service, bridge, or indexing changes: `cargo test -p tao-sdk-service --release` and `cargo test -p tao-sdk-bridge --release`
- Fixture or graph/base parity changes: use [`fixtures/README.md`](fixtures/README.md), rerun the parity refresh flow, and keep [`fixtures/graph/expected/`](fixtures/graph/expected/) in sync with CLI snapshot tests
- Benchmark or performance changes: rerun the relevant suites from [`scripts/bench.py`](scripts/bench.py) and [`scripts/bench.py --suite budget`](scripts/bench.py); reports land under [`.benchmarks/reports/`](.benchmarks/reports/) with a `latest` symlink
- There is no tracked CI workflow directory at the repo root today, so local script/test output is the completion bar

## 10. Further Reading

- [`scripts/tests/test_paths.py`](scripts/tests/test_paths.py) for the generic repository-local output and live-vault path guard expectations the repo actively tests
