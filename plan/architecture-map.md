## Tao Architecture Map

## Runtime Layers
- CLI layer: `crates/tao-cli`
- Bridge layer: `crates/tao-sdk-bridge`
- Service layer: `crates/tao-sdk-service`
- Domain helpers: `crates/tao-sdk-{bases,links,markdown,properties,search,vault,watch,config,core}`
- Persistence layer: `crates/tao-sdk-storage`
- Bench layer: `crates/tao-bench`
- Native app layer: `apps/tao-macos`

## Primary Read Flow
1. `tao-cli` parses command and maps request.
2. Request is executed in `tao-sdk-service` or `tao-sdk-search`.
3. Service calls `tao-sdk-storage` repositories for indexed data.
4. Results are returned through bridge/CLI envelope.

## Primary Index Flow
1. `tao-sdk-vault` scans vault filesystem and fingerprints files.
2. `tao-sdk-markdown` and `tao-sdk-properties` parse markdown/frontmatter.
3. `tao-sdk-links` resolves wikilinks/anchors.
4. `tao-sdk-service` writes normalized rows through `tao-sdk-storage`.

## Base Flow
1. `.base` YAML parsed by `tao-sdk-bases`.
2. Planner output is consumed by service execution.
3. Rows are materialized from storage-backed metadata indexes.

## Graph Flow
1. Link extraction and resolution happen during indexing.
2. Resolved/unresolved links are persisted in storage tables.
3. Graph commands (`outgoing`, `backlinks`, `unresolved`, traversal) run from indexed edges.

## Benchmark and Fixture Flow
1. `scripts/fixtures.sh` generates deterministic vaults under `vault/generated`.
2. `scripts/bench.sh` and `scripts/budgets.sh` run read-only benchmarks on generated fixtures.
3. Reports are written under `.benchmarks/reports`.
4. Safety guard script rejects personal paths and external vault roots.
