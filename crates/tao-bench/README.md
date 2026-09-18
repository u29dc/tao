# Tao benchmarks

The Rust harness measures SDK parser, resolver, search, graph, indexed-runtime
startup and bridge workloads. The Python standard-library driver measures real
CLI processes with argument vectors and an unadjusted monotonic wall clock.
Neither requires a shell timing calibration or `hyperfine`.

## Controlled process measurements

```sh
python3 -B scripts/bench.py --suite cli --profile 1k --runs 25 --warmup 5
python3 -B scripts/bench.py --suite daemon --profile 5k --variant representative
python3 -B scripts/bench.py --suite budget --profile 5k --runs 25 --warmup 5
```

`--mode direct`, `--mode daemon-hit`, and `--mode daemon-miss` distinguish direct
execution, repeated cached results, and daemon computation with result-cache
bypass. The daemon comparison measures all three modes. Each measured response
must confirm its backend/cache state in runtime metadata. Observational and
maintenance operations always bypass result caching.

Synthetic PDF extraction and the daemon's initial refresh complete before
cache priming and timing. Initialization responses are retained in the report;
measured samples still fail if their reported backend or cache state changes.

Every run has its own report, fixture, database and socket paths. All generated
outputs remain repository-local. `--skip-generate --fixture-vault PATH` reuses a
fixture only after its manifest, requested seed/profile/variant and indexed
metadata are validated. `--skip-build` uses existing release binaries. The driver
runs foreground SDK scenarios sequentially; do not run competing benchmark suites
when making comparative performance claims.

Reports preserve raw samples, result-source identity checks, actual mode, output
bytes, compiler/machine/checkout identity, and the fixture manifest. Fewer than 20
samples do not produce a p95 claim. A separate resource probe reports client peak
RSS in bytes and an explicitly labeled daemon RSS snapshot; the latter is not a
peak-memory measurement. Drift fixtures are restored even on failure.

An optional versioned configuration supplied with `--budget-config PATH` (or
stored locally at `.benchmarks/budgets.json`) defines separate 1k/5k profiles for
direct processes, daemon cache hits and daemon bypass. It records baseline p50s,
fixture hashes and provenance alongside case-specific thresholds. An unmeasured
variant/mode or changed fixture hash fails instead of reusing an unrelated budget.
Budget gates require at least 20 samples. Machine-specific budgets remain local;
this repository does not ship a calibrated performance baseline.

Legacy configuration also accepts `warm_read_p50_ms`, `graph_read_p50_ms`,
`base_read_p50_ms`, `query_read_p50_ms` and `case_budgets_p50_ms`. The individual
keys are `query-docs`, `query-base`, `query-graph-path`, `graph-links`,
`graph-path`, `graph-walk`, `search-project-context` and `meta-tags`. Unknown keys,
invalid thresholds and missing/empty/invalid samples fail. The legacy 10 ms
fallback is only available for daemon cache-hit cases when no configuration is
present; it is not an uncached service SLO.
`--budget-ms` explicitly overrides all case budgets. Ratify budgets after measuring
corrected representative workloads on the target machine.

## Fixed mixed-format relevance gate

```sh
python3 -B scripts/bench.py --suite relevance
```

The independent authored corpus contains Markdown aliases and titles, Unicode,
TXT evidence, a 24-page native PDF, inventory-only assets, a long repetitive note,
and 120 irrelevant lexical distractors. Eighteen fixed judgments are recorded
before execution. The gate waits for extraction, validates source revision and
line/page evidence, and checks strongest-entity top-1, recall at 10, MRR, graded
NDCG at 10, limit-prefix stability at 1/3/10/100, and direct/cached/bypass parity.
It preserves every response and confirms originals are unchanged. This small
synthetic set is a semantic regression gate, not an estimate of relevance for a
personal vault or a latency benchmark. Expected answers are never regenerated
from tool output.

## SDK measurements

```sh
python3 -B scripts/bench.py --suite sdk --profile 5k --variant representative
cargo test -p tao-bench --release
```

SDK reports preserve sub-microsecond samples. Parser input is distributed across
the full included manifest, including its largest Markdown file. Every timed
sample records its source path and byte size. File I/O is outside parser timing;
source cloning is inside it. Resolver latency is per
256-link batch. Graph/query comparisons distinguish a reused connection from a
new connection over a warm filesystem. Startup means opening an already-indexed
bridge runtime, not process startup or initial indexing. Benchmark temporary
vaults and report files remain inside the repository.

## Live reads

```sh
python3 -B scripts/bench.py --suite live --live-vault /path/to/vault --mode daemon-miss
```

A live run reads original content and creates an isolated index in its ignored
repository report directory. It does not edit source files. Never use fixture
generation or general QA against a personal vault. Live probes and their results
may contain private paths or phrases: keep them untracked.

Optional `--live-commands FILE` accepts JSONL records containing an `id` and a
literal `args` array, for example `{"id":"phrase","args":["search","example"]}`.
Shell fragments, redirection and arbitrary executable commands are not supported.

## Tooling verification

`bun run util:scripts` checks budget failure semantics, runtime labeling, fixture
identity, frontmatter, golden safety, package extraction, installation rollback,
and cleanup scope without building or touching an installed tool.

Build and packaging are separate from explicit installation. `bun run build`
only creates/verifies the local release executable; `bun run release:cli`
packages it; `bun run release:install -- --out PATH` installs it atomically and
verifies the installed path. `bun run util:test` isolates Rust tests from ambient
Tao configuration and keeps temporary state under `target/`.
