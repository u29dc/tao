# Test fixtures

Each family separates source files from expected results. Tests copy only the
`vault/` directory to disposable repository-local storage before indexing.

```text
fixtures/
  conformance/vault/    Markdown, metadata, malformed frontmatter and Bases
  base/vault/           Compact Base filtering and property examples
  graph/vault/          Links, fragments, diagnostics and attachment inventory
  graph/expected/       Reviewed JSON payloads for graph command contracts
```

Paths and source bytes are stable. Graph expectations are reviewed contracts,
never generated from actual output and automatically accepted. Repository-local
fixtures intentionally include invalid metadata and unsupported or malformed
assets so failure behavior remains covered.

## Coverage and ownership

- `conformance`: same-folder/cross-folder ambiguity, unresolved links, heading and
  block fragments, scalar/list properties, aliases, malformed YAML, valid and
  invalid Base definitions. Used by the service's `tests/conformance.rs`.
- `base`: compact project/meeting properties and a filtered table view. Available
  through the parity-copy workflow for focused tests and manual exploration.
- `graph`: CLI graph snapshot contracts, including unresolved-link evidence and
  inventory-only attachments. Used by `cli_impl/tests/graph.rs`.
- Generated synthetic and relevance corpora: owned by `scripts/fixtures.py` and
  `scripts/relevance.py`, with fixed semantics checked before measurements.

## Generate disposable copies

```sh
python3 -B scripts/fixtures.py --profile parity
python3 -B scripts/fixtures.py --profile 1k --variant tiny
python3 -B scripts/fixtures.py --profile 5k --variant representative
```

Outputs default to `target/fixtures/`. Parity copies preserve the family layout,
including sibling expectations; index `target/fixtures/graph/vault`, not its
parent. Synthetic vaults are named `vault-1k`, `vault-5k`, or have a
`-representative` suffix. Representative notes add varied text lengths, a dense
project hub, Unicode TXT, a native PDF, duplicate names and an unsupported asset.
They do not claim to reproduce every live-vault format or OCR failure.

Each managed output has a `.fixture-manifest.json` recording generator version,
seed, content identity, counts, metadata distributions and expected project
values. Generation refuses tracked content, symlink destinations and nonempty
unmanaged directories. Validate after ingestion with:

```sh
python3 -B scripts/fixtures.py validate target/fixtures/vault-1k --database PATH
```

## Update graph expectations

1. Generate a disposable parity copy and index its `graph/vault/` directory.
2. Capture the relevant command's JSON `data` payload into a candidate file.
3. Compare the candidate with `graph/expected/` and review semantic differences.
4. Update only the intended checked-in expectation, then run:

```sh
bun run util:test -- graph_snapshot_contracts_match_golden_outputs
```

Never generate fixtures or run automated QA against a personal vault. Tao ignores
root `.taoignore` patterns rather than Git ignore rules when scanning a vault.
