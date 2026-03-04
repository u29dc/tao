# Phase23 Performance Runbook

## Core Gate

```bash
./scripts/budgets.sh
```

`budgets.sh` now:

1. builds release benchmark/CLI binaries if missing
2. generates deterministic 10k fixture vault
3. validates fixture realism and safety
4. seeds index (`vault open`, `vault reindex`)
5. runs bridge/ffi/startup latency checks via `tao-bench`
6. runs `hyperfine` checks for:
   - `query --from docs`
   - `graph unresolved`

## Target Budgets

- query/docs mean <= 10ms
- graph/unresolved mean <= 10ms
- startup budget remains separately enforced by startup report threshold

## Reports

Outputs are written to `.benchmarks/reports/`:

- `bridge-call-budgets.json`
- `ffi-call-budgets.json`
- `startup-budgets.json`
- `query-docs-hyperfine.json`
- `graph-unresolved-hyperfine.json`
