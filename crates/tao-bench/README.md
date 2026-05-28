## Crate

`tao-bench`

## Purpose

Run deterministic benchmark scenarios for bridge, startup, query, graph, parser, search, and CLI hot paths.

## Public API

- Binary: `tao-bench`
- Scenario-driven benchmark execution with JSON report output
- `scripts/bench.sh --suite live` runs the high-value real-vault CLI benchmark matrix with `hyperfine`, including reindex dry-run, reconcile reindex, graph audits, and generic search shapes. Provide the vault path at runtime with `--live-vault` or `TAO_BENCH_LIVE_VAULT`.
- Private live search/path probes belong in `.benchmarks/live-commands.txt` using `id|command` lines and placeholders such as `{tao}`, `{vault}`, `{db}`, and `{socket}`. `.benchmarks/` is gitignored.

## Internal Design

- Scenario registry maps benchmark names to callable workloads.
- Reuses SDK/service components to benchmark real execution paths.
- `--scenario search` accepts `--kind`, `--scope`, `--ext`, `--context`, `--depth`, `--path`, and `--limit` so service-level search benchmarks can cover docs, files, properties, tasks, graph, and context expansion without adding public CLI commands.

## Data Flow

Scenario args -> benchmark loop -> latency samples -> JSON report for scripts/budget gates.

## Dependencies

- Internal: `tao-sdk-bridge`, `tao-sdk-service`, `tao-sdk-links`, `tao-sdk-markdown`, `tao-sdk-search`
- External: `clap`, `serde`, `serde_json`, `rusqlite`, `anyhow`, `tempfile`

## Testing

- `cargo test -p tao-bench --release`

## Limits

- Synthetic fixtures are limited to 1k and 5k profiles. Real-vault benchmarks are allowed for read-only CLI behavior and internal index/database state writes.
- Do not commit real-vault paths, search strings, names, meeting titles, or benchmark reports containing private data.
