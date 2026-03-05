## Execution Checklist

- [ ] Confirm `scripts/safety.sh --check-repo` passes before any task execution.
- [ ] Execute tickets in dependency-safe order from `plan/tickets.csv`.
- [ ] For each ticket, implement complete behavior and tests.
- [ ] Run ticket-relevant commands and record command outputs in `plan/progress.md`.
- [ ] Run `bun run util:check` at regular checkpoints and after each major cluster.
- [ ] Keep commits atomic and mapped to ticket IDs.
- [ ] Never use real personal vaults or non-repo test paths.
