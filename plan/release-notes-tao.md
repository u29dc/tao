# Tao Transition Release Notes

Date: 2026-03-04
Scope: rename migration, native runtime bridge, config/bootstrap defaults, and app UX simplification.

## Highlights

- Project identity fully migrated from `obs` to `tao` (repo, binaries, package metadata, env names, runtime paths).
- CLI now supports vault-root-first operation:
  - `--vault-root` is sufficient for bootstrap and command execution.
  - sqlite path auto-resolves and migrations auto-apply.
  - `--db-path` remains available as an advanced override.
- Native bridge runtime is now direct and persistent:
  - `tao-sdk-bridge` exports a long-lived `TaoBridgeRuntime`.
  - Swift client uses generated bindings with runtime-handle caching per vault/db key.
  - subprocess-per-call bridge execution has been removed from the app path.
- macOS app shell simplified:
  - one file-tree sidebar
  - one note reader pane with front matter properties + markdown content
  - settings window for vault folder selection
  - debug-oriented panes removed
- TUI reset to explicit placeholder shell while the next TUI phase is deferred.

## 1. Repository + Binary Naming

- New repository: `git@github.com:u29dc/tao.git`
- Primary CLI binary: `tao`
- TUI binary: `tao-tui`
- Bridge crate/binary namespace: `tao-sdk-bridge`

## 2. Environment and Runtime Paths

- Legacy names moved to `TAO_*`.
- Default runtime storage is vault-scoped under `.tao/`.
- SQLite auto-bootstrap happens on first use when missing.

## 3. Config Resolution

Resolution precedence:

1. SDK defaults
2. root `config.toml` (repo/workdir root resolution)
3. vault `config.toml`
4. environment variables
5. explicit CLI/runtime overrides

Bootstrapping behavior:

- root and vault config files are created on first use when absent.
- root configuration resolves to git-repo root when invoked from nested project paths.

## 4. Performance and Validation

- Benchmark reports now write to `.benchmarks/reports/` (gitignored).
- Budget gate covers bridge, ffi, and startup scenarios through `./scripts/budgets.sh`.
- Full release-quality validation command remains:
  - `bun run util:clean && bun run util:check`

## 5. Compatibility and Risk Notes

- Existing benchmark snapshots under legacy `bench/reports/` were removed; regenerate locally as needed.
- Workflows are currently disabled manually in GitHub to control usage; re-enable intentionally when needed.
