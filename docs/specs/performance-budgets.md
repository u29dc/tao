# Performance Budget Contract

## Purpose

Define non-negotiable latency, throughput, and memory budgets for v1.

## Environment Baseline

- Platform: macOS (Apple Silicon).
- Workload profiles: 1k, 5k, 10k, 25k notes.
- Fixture source: synthetic corpus + sampled real vault snapshots.

## Runtime Budgets

| Metric | Target | Gate Type |
| --- | --- | --- |
| App interactive warm start | <= 300ms | hard |
| App interactive cold start | <= 900ms | hard |
| Open note p50 | <= 30ms | hard |
| Open note p95 | <= 120ms | hard |
| Search p50 | <= 15ms | hard |
| Incremental index apply p50 | <= 120ms | hard |
| Full reindex 5k notes | <= 12s | soft |
| Memory @ 5k notes | <= 350MB process total | soft |

## Measurement Rules

- Collect at least 10 runs per benchmark scenario.
- Report p50, p95, and max for latency distributions.
- Record host metadata (CPU, RAM, OS version).
- Use same fixture versions across comparisons.

## Regression Policy

- Any hard budget regression fails CI.
- Soft budget regression opens blocker ticket with remediation plan.
- Perf changes must include before/after benchmark evidence in `progress.md`.

## Instrumentation Requirements

- Rust microbench crate for parser/resolver/db hot paths.
- Integration benchmark harness over vault fixtures.
- Swift startup/open/search traces captured and archived.

## SQLite Profile

- Runtime pragma profile is documented in `docs/db/sqlite-pragma-profile.md`.
- Any pragma change requires benchmark evidence and ticket-level regression notes in `progress.md`.

## CI Integration

- `PERF-008` gates enforce budget checks on tracked benchmarks.
- `scripts/check-perf-budgets.sh` is the canonical budget gate entrypoint in `rust-ci`.
- Bridge boundary benchmarks are emitted to `bench/reports/bridge-call-budgets.json`.
- Startup benchmark budgets are emitted to `bench/reports/startup-budgets.json`.
- Flaky benchmarks require quarantine annotation and follow-up fix ticket.
