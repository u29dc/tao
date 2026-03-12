## Fixture Profiles

`graph-parity` and `base-parity` are deterministic compact fixtures used by snapshot and parity tests.

## Refresh Workflow

1. Copy fixture profile to a temporary directory.
2. Run `tao vault open` and `tao vault reindex`.
3. Capture command JSON `data` payloads for snapshot-covered commands.
4. Write payloads into `expected/*.json` under the fixture profile.
5. Run `cargo test -p tao-cli --release graph_snapshot_contracts_match_golden_outputs`.

## Safety

These fixtures are repository-local. Do not run parity snapshot generation against real vaults.
