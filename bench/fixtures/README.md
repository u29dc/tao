# Benchmark Fixtures

This folder stores synthetic and sampled fixtures used by performance tickets.

## Planned Profiles

- `vault-1k/` : 1,000 markdown notes
- `vault-5k/` : 5,000 markdown notes
- `vault-10k/` : 10,000 markdown notes
- `vault-25k/` : 25,000 markdown notes

## Generation

Run:

```bash
scripts/fixtures.sh
```

This script creates deterministic synthetic content for repeatable benchmarking.
