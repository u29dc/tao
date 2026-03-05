## Crate

`tao-bench`

## Purpose

Run deterministic benchmark scenarios for bridge, startup, query, graph, and parser hot paths.

## Public API

- Binary: `tao-bench`
- Scenario-driven benchmark execution with JSON report output

## Internal Design

- Scenario registry maps benchmark names to callable workloads.
- Reuses SDK/service components to benchmark real execution paths.

## Data Flow

Scenario args -> benchmark loop -> latency samples -> JSON report for scripts/budget gates.

## Dependencies

- Internal: `tao-sdk-bridge`, `tao-sdk-service`, `tao-sdk-links`, `tao-sdk-search`
- External: `clap`, `serde`, `serde_json`, `rusqlite`, `anyhow`, `tempfile`

## Testing

- `cargo test -p tao-bench --release`

## Limits

- Benchmarks run only against repository-local synthetic fixtures.
