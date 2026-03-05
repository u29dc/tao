# Repository Audit Report

**Date:** 2026-03-05
**Scope:** Full read-only deep audit of `u29dc/tao`
**Auditor:** Automated (Claude Opus 4.6)
**Commit range:** All 50 commits (2026-03-03 to 2026-03-05)

---

## A) Executive Summary

1. **Well-architected SDK-first Rust workspace** with 15 domain crates, strict `unsafe_code = "forbid"`, and clean dependency layering from core primitives through storage, services, bridge, to CLI.
2. **Zero SQL injection risk** — every SQL query across the codebase uses `rusqlite` parameterized placeholders (`?1`, `?2`, `params![]`). No string interpolation of user input into SQL anywhere.
3. **Strong path traversal protection** — vault path canonicalization, `..` component rejection, vault boundary enforcement, and a dedicated `safety.sh` guard prevent access to forbidden personal paths.
4. **Three monolith files dominate the codebase** — `legacy.rs` (6,110 lines), `cli_impl.rs` (5,750 lines), and `bridge/lib.rs` (1,891 lines) together account for 36% of all Rust code. These carry maintainability and review risk despite functional correctness.
5. **Graph operations load entire tables into memory** — `components_page`, `walk`, and several graph algorithms call `FilesRepository::list_all()` and `LinksRepository::list_resolved_pairs()`, creating O(n) memory pressure proportional to vault size.
6. **YAML deserialization of untrusted frontmatter lacks resource limits** — `serde_yaml::from_str::<Value>()` on user-controlled markdown frontmatter is susceptible to YAML bomb / billion laughs DoS.
7. **261 Rust tests + 15 Swift tests** provide good coverage of core paths, but no fuzz testing, no property-based testing, and some crates (config, watch) have minimal test depth.
8. **Comprehensive benchmark infrastructure** with hyperfine CLI matrix (30 commands), SDK scenario harness, daemon comparison gates, and budget enforcement — a standout strength.
9. **No async runtime** — the entire codebase is synchronous. The bridge uses `Mutex<BridgeKernel>` for thread safety, which serializes all bridge calls. This is adequate for current single-client SwiftUI use but would bottleneck under concurrent access.
10. **Overall confidence: HIGH** for a local-first single-user tool. The codebase is functionally complete for its stated phase 24 scope, with good safety discipline. Main risks are scalability ceilings on large vaults and the YAML DoS vector.

---

## B) Architecture Map

```
┌─────────────────────────────────────────────────────┐
│              SwiftUI macOS Application               │
│  (TaoMacOSApp.swift, TaoBridgeClient.swift)          │
└──────────────────────┬──────────────────────────────┘
                       │ UniFFI bindings (generated)
┌──────────────────────▼──────────────────────────────┐
│         tao-sdk-bridge (TaoBridgeRuntime)            │
│  Mutex<BridgeKernel>, BridgeEnvelope<T>, DTOs        │
│  Persistent handle per (vault_root, db_path)         │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│           tao-sdk-service (Orchestrator)              │
│  FullIndexService, NoteCrudService, BacklinkGraph,   │
│  BaseTableExecutor, PropertyQuery, ReconcileService  │
└───┬──────┬──────┬──────┬──────┬──────┬──────────────┘
    │      │      │      │      │      │
    ▼      ▼      ▼      ▼      ▼      ▼
 links  markdown props  bases search  storage
 (SDK)  (SDK)    (SDK)  (SDK)  (SDK)   (SQLite)
    │                                    │
    └──────────┬─────────────────────────┘
               │
      ┌────────▼─────────┐     ┌──────────────┐
      │  tao-sdk-vault   │     │ tao-sdk-core │
      │  (FS scan, path  │     │ (events, text│
      │   canonicalize)  │     │  utilities)  │
      └──────────────────┘     └──────────────┘

┌─────────────────────────────────────────────────────┐
│                tao-cli (clap binary)                  │
│  Vault│Doc│Base│Graph│Meta│Task│Query commands        │
│  JSON envelope, daemon socket, streaming output      │
│  Depends on: bridge, search, service, storage, etc.  │
└─────────────────────────────────────────────────────┘

┌───────────────────────────────┐  ┌──────────────────┐
│  tao-bench (benchmark harness)│  │  tao-tui (stub)  │
└───────────────────────────────┘  └──────────────────┘
```

**Key integration points:**
- CLI ↔ SQLite: opens `Connection` directly, passes to repository/service calls
- Bridge ↔ SQLite: `BridgeKernel` owns `Connection`, wrapped in `Mutex` by `TaoBridgeRuntime`
- CLI ↔ Daemon: Unix domain socket IPC with JSON envelope protocol
- Swift app ↔ Bridge: UniFFI-generated bindings, async Swift wrapper
- Config: layered TOML (global → root → vault → env → CLI flags)
- Migrations: forward-only with BLAKE3 checksum guards, 7 migrations

---

## C) Key Flows

### C1. Vault Open + Index Bootstrap
1. CLI parses `--vault-root`, resolves config via `SdkConfigLoader` → `crates/tao-sdk-service/src/config.rs`
2. Opens SQLite connection, runs `run_migrations()` → `crates/tao-sdk-storage/src/lib.rs:239`
3. Executes `FullIndexService::run()` → `crates/tao-sdk-service/src/legacy.rs` (ingest pipeline)
4. Scans vault filesystem via `VaultScanService` → `crates/tao-sdk-vault/src/scan.rs`
5. Parses markdown, extracts frontmatter, wikilinks, tasks → markdown/properties/links crates
6. Persists to SQLite via batch insert in transaction → `crates/tao-sdk-storage/src/`

### C2. Unified Query Execution
1. CLI `query --from <scope>` parsed → `crates/tao-cli/src/cli_impl.rs:~line 45`
2. Logical plan built via `LogicalPlanBuilder` → `crates/tao-sdk-search/src/logical_plan.rs`
3. Physical plan compiled via `PhysicalPlanBuilder` → `crates/tao-sdk-search/src/physical_plan.rs`
4. Optimizer applies → `crates/tao-sdk-search/src/optimizer.rs`
5. Execution against SQLite via scope-specific adapter → `crates/tao-sdk-search/src/execution.rs`
6. Results serialized to JSON envelope → CLI stdout

### C3. Bridge Startup Bundle (macOS app)
1. Swift calls `TaoBridgeRuntime.new(vault_root, db_path)` → `crates/tao-sdk-bridge/src/runtime.rs:35`
2. Acquires `Mutex<BridgeKernel>` lock → `runtime.rs:203-211`
3. `ensure_indexed()` triggers full index if needed → `bridge/lib.rs`
4. `vault_stats()` + `notes_list()` combined into single startup bundle
5. Serialized to JSON string, returned across FFI boundary

### C4. Graph Walk Traversal
1. CLI `graph walk --path <note> --depth N` → `cli_impl.rs`
2. Loads ALL files via `FilesRepository::list_all()` → builds `path_by_id` map
3. BFS from root using `LinksRepository` adjacency lookups → `legacy.rs:~3487`
4. Optional folder-edge overlay adds parent/sibling edges
5. Results windowed by `--limit` and serialized

### C5. Note CRUD with Rollback
1. `NoteCrudService::create_note()` → `legacy.rs:281`
2. Validates path (traversal check, vault boundary)
3. Writes file to filesystem
4. Indexes note in SQLite within transaction
5. On DB failure: compensating rollback deletes the filesystem file
6. Returns `CoordinatorRollback` error if rollback also fails

---

## D) Intent/Requirements Compliance Matrix

| Requirement (from CLAUDE.md / tickets) | Status | Evidence | Notes/Risks |
|---|---|---|---|
| `unsafe_code = "forbid"` workspace-wide | **Done** | `Cargo.toml:44` | Enforced by compiler |
| CLI compact surface: vault/doc/base/graph/meta/task/query | **Done** | `cli_impl.rs:66-99` Commands enum | Clean implementation |
| Compatibility aliases (note, links, properties, bases, search) | **Divergent** | CLI-001 ticket says "remove legacy aliases" and they return unknown-command; CLAUDE.md §10 says "aliases remain supported" | CLAUDE.md contradicts CLI-001 completion |
| JSON envelope `{ ok, value, error }` | **Done** | Bridge: `BridgeEnvelope<T>` at `bridge/lib.rs:131`; CLI: envelope functions in `cli_impl.rs` | Consistent across both surfaces |
| Write gate (`--allow-writes`) | **Done** | CLI: `cli_impl.rs:57`; Bridge: `note_put_with_policy` at `runtime.rs:106` | Default false, explicit opt-in |
| Query scopes: docs/graph/task/meta:*/base:* | **Done** | `cli_impl.rs` QueryArgs parsing + search adapters | Full scope coverage |
| Frontmatter + body wikilink indexing | **Done** | `tao-sdk-links/src/lib.rs`, GRAPH-001 ticket completed | Centralized tokenization |
| Graph diagnostics (unresolved/deadends/orphans/components/walk) | **Done** | `cli_impl.rs:121-141` GraphCommands; `legacy.rs` implementations | Full coverage |
| Case-policy aware canonicalization | **Done** | `tao-sdk-vault/src/path.rs`; GRAPH-003 ticket completed | Threaded from config |
| Deterministic synthetic fixtures | **Done** | `scripts/fixtures.sh` with seed + validation | 5 profiles, comprehensive invariant checks |
| Forbidden personal path scanning | **Done** | `scripts/safety.sh`, integrated into util:check | Two-layer defense |
| Performance budgets (query ≤10ms, graph ≤10ms) | **Done** | `scripts/bench.sh`, `plan/perf-budgets.json` | Enforced in benchmark gates |
| SQLite WAL mode + pragma tuning | **Done** | `tao-sdk-storage/src/lib.rs:120-128` | 7 pragmas correctly set |
| Migration checksum guards | **Done** | `tao-sdk-storage/src/lib.rs:239-300` | BLAKE3 checksums, forward-only |
| Signed commits | **Partial** | Commit convention followed; signing not verifiable in this env | Policy documented |
| `cargo fmt` / `clippy -D warnings` | **Done** | `package.json:24-25` util:format and util:lint | Part of util:check gate |
| Base typed coercion, filter operators, sorting, grouping, rollups | **Done** | BASE-001 through BASE-010 all completed | Full base engine |
| Query planner (logical/physical/optimizer) | **Done** | `tao-sdk-search/src/` 7-module decomposition | Clean pipeline |
| Daemon warm runtime | **Done** | `cli_impl.rs` DaemonCommands; CLI-004 stale socket handling | Unix socket IPC |
| Config precedence chain | **Done** | `tao-sdk-service/src/config.rs`, `tao-sdk-config/src/lib.rs` | defaults < global < root < vault < env < CLI |
| Crate READMEs with uniform template | **Done** | DOC-001/DOC-002 completed | Verified template exists |

---

## E) Findings Table

| # | Area | Severity | Finding | Evidence | Recommendation | Effort |
|---|---|---|---|---|---|---|
| F1 | Performance | **High** | Graph operations (`components_page`, `walk`, `shortest_path`) load ALL files and ALL resolved link pairs into memory via `FilesRepository::list_all()` and `LinksRepository::list_resolved_pairs()`. For 25k-note vaults, this means 25k+ file records and 100k+ link pairs materialized simultaneously. | `legacy.rs:3429-3444` (components), `legacy.rs:3487` (walk) | Implement lazy/streaming adjacency lookups or maintain a persistent in-memory graph index that's incrementally updated. For components, consider database-side connected component detection or bounded subgraph loading. | Large |
| F2 | Security | **High** | YAML frontmatter deserialization uses `serde_yaml::from_str::<Value>()` without resource limits. Malicious markdown files with YAML bomb anchors (`&a [*a, *a, ...]`) can cause exponential memory allocation via recursive expansion. | `tao-sdk-properties/src/lib.rs:64` | Add pre-deserialization size limits on frontmatter content (e.g., reject >64KB frontmatter). Consider switching to a YAML parser with built-in recursion depth limits, or pre-validate anchor usage before parsing. | Medium |
| F3 | Maintainability | **High** | Three files account for 13,751 lines (36% of Rust code): `legacy.rs` (6,110), `cli_impl.rs` (5,750), `bridge/lib.rs` (1,891). Despite ticket REF-001/REF-002/REF-003 being marked "done", these files remain monolithic. `legacy.rs` contains 10+ distinct service types in a single file. | Line counts measured directly | Complete the modularization: split `legacy.rs` into per-service modules (NoteCrudService, PropertyQueryService, BacklinkGraphService, etc.), split `cli_impl.rs` command handlers into `commands/` submodules (partially done but bulk remains), extract `BridgeKernel` methods from `bridge/lib.rs`. | Large |
| F4 | Performance | **Med** | `PropertyQueryService::query` loads all rows matching a key into memory, then applies case-insensitive `contains()` filtering and sorting in Rust instead of pushing these operations to SQL. | `legacy.rs:1271-1306` | Push `LIKE` filtering and `ORDER BY` to SQL query. Use `LOWER(value) LIKE '%' || ?1 || '%'` pattern for case-insensitive search. | Medium |
| F5 | Performance | **Med** | `compute_base_table_metadata_digest` reads ALL rows from `files`, `properties`, and `bases` tables to compute a cache invalidation digest on every cache check. | `legacy.rs:2740-2776` | Replace with a monotonic version counter or `MAX(updated_at)` check, which is O(1) instead of O(n). | Medium |
| F6 | Performance | **Med** | `resolve_base_by_id_or_path` calls `BasesRepository::list_with_paths()` loading all bases, then does a linear scan with `.find_map()` to locate one base by ID or path. | `bridge/lib.rs:1221` | Add a `get_by_id_or_path()` SQL query to `BasesRepository` using `WHERE id = ?1 OR normalized_path = ?1 LIMIT 1`. | Quick win |
| F7 | Concurrency | **Med** | `TaoBridgeRuntime` wraps `BridgeKernel` in `Mutex`, serializing all bridge calls. The `BridgeKernel` owns a single bare `rusqlite::Connection`. Under concurrent Swift UI access (e.g., background refresh + foreground navigation), this creates a bottleneck. | `runtime.rs:26-30`, `runtime.rs:203-211` | For current single-client use this is acceptable. For future multi-window or background processing, consider a connection pool (e.g., `r2d2`) or `RwLock` with read-only connections for queries. | Large |
| F8 | Correctness | **Med** | `apply_initial_schema()` silently swallows duplicate-column errors for specific migrations (0005, 0007) but does not record them in `schema_migrations`. This means subsequent `run_migrations()` calls will re-attempt and re-swallow these migrations indefinitely. | `tao-sdk-storage/src/lib.rs:131-153` | Either remove `apply_initial_schema` (it appears to be a fast-path shortcut) or ensure it records applied migrations. The `run_migrations()` path handles this correctly. | Quick win |
| F9 | Spec Drift | **Med** | CLAUDE.md §10 states "Compatibility aliases remain supported: note, links, properties, bases, search" but ticket CLI-001 ("Remove legacy alias command paths") is marked done. The aliases were removed, contradicting the documented contract. | `CLAUDE.md:§10`, `plan/run-state.json` CLI-001=done | Update CLAUDE.md §10 to remove the alias support claim, or re-implement aliases as thin dispatchers. | Quick win |
| F10 | Reliability | **Med** | Daemon socket handling has no authentication. Any local process can connect to the Unix domain socket and execute commands, including write operations if `--allow-writes` is passed. | `cli_impl.rs:42-44` daemon socket path | For a local-only tool this is low risk. If daemon use expands, add a shared-secret token file (permissions 0600) that clients must present. | Medium |
| F11 | Testing | **Med** | No fuzz testing or property-based testing exists. The YAML/markdown parsing pipeline processes untrusted user content and would benefit from fuzzing. Several crates have minimal test depth: `tao-sdk-config` (config loading edge cases), `tao-sdk-watch` (filesystem reconciliation). | Grep for `#[test]` across crates; no `proptest`, `quickcheck`, `cargo-fuzz` in deps | Add `cargo-fuzz` targets for markdown parsing, YAML frontmatter extraction, wikilink tokenization, and `.base` document parsing. Add property-based tests for deterministic sort/filter behavior. | Medium |
| F12 | Data | **Low** | Two redundant SQLite indexes exist: `idx_files_match_key` duplicates the implicit index from `UNIQUE(match_key)`, and `idx_tasks_file_line` duplicates the implicit index from `UNIQUE(file_id, line_number)`. | `migrations/0001_init.sql`, `migrations/0003_tasks.sql` | Remove redundant indexes to reduce write overhead (minor). | Quick win |
| F13 | Performance | **Low** | Graph algorithms (`weak_components`, `strong_components`) perform extensive `String::clone()` operations when building adjacency lists and visited sets. | `legacy.rs:3684-3801` | Use integer-indexed nodes (`HashMap<usize, Vec<usize>>`) with a separate `id_to_path` map to eliminate string cloning in inner loops. | Medium |
| F14 | Performance | **Low** | `ingest_entries` in the indexing pipeline processes files sequentially. For large vaults (10k+), parallel parsing via `rayon` could provide significant speedup since markdown parsing is CPU-bound. | `legacy.rs:119-160` | Wrap the ingest loop in `rayon::par_iter()` and collect results, then apply batched DB writes. Note: other parts of the pipeline already use rayon. | Medium |
| F15 | Observability | **Low** | `tracing` is a workspace dependency but no structured log statements (`tracing::info!`, `tracing::warn!`) were found in production code paths. The subscriber is configured but logging is effectively silent. | `Cargo.toml:33-34` tracing deps; no `tracing::` calls in service/bridge code | Add structured tracing spans for key operations: index pipeline stages, query execution, daemon lifecycle, migration runs. | Medium |
| F16 | DX | **Low** | `BridgeKernel.connection` field appears to be `pub` (tests directly access `kernel.connection` to insert records). This leaks internal state beyond the intended API boundary. | `bridge/lib.rs` test code accessing `kernel.connection` | Change to `pub(crate)` and use proper test helper methods. | Quick win |
| F17 | DX | **Low** | `load_applied_checksums` and `load_applied_checksums_connection` in `tao-sdk-storage/src/lib.rs` are near-identical functions differing only in whether they take `&Transaction` or `&Connection`. | `tao-sdk-storage/src/lib.rs:312-360` | Use a generic `impl Deref<Target=Connection>` parameter or the `rusqlite::Connection` trait to unify both functions. | Quick win |
| F18 | Reliability | **Low** | `bridge_events` table uses `INTEGER PRIMARY KEY` with a redundant explicit index `idx_bridge_events_id`. | `bridge/lib.rs:1277` | Remove the redundant index. SQLite automatically indexes `INTEGER PRIMARY KEY`. | Quick win |

---

## F) Assumptions & Invariants

| # | Assumption/Invariant | Where Encoded | How to Validate | Risk if Violated |
|---|---|---|---|---|
| A1 | Vault paths never contain `..` components | `tao-sdk-vault/src/path.rs` validation | Path canonicalization rejects `ParentDir` | Path traversal attack |
| A2 | SQLite database is exclusively owned by one process | Implicit (no locking protocol) | `PRAGMA busy_timeout = 5000` provides partial mitigation | Corruption under concurrent writes |
| A3 | All markdown files use UTF-8 encoding | `fs::read_to_string()` calls throughout | Would panic/error on invalid UTF-8 | Index failure on non-UTF-8 files |
| A4 | Migration SQL checksums never change after release | `BLAKE3` hash comparison in `run_migrations()` | `MigrationRunnerError::ChecksumMismatch` | Database startup failure |
| A5 | `match_key` uniquely identifies a file across the vault | `UNIQUE(match_key)` constraint | SQLite enforces at insert | Duplicate file records |
| A6 | Wikilink targets are case-insensitive by default | `CasePolicy` from config, default insensitive | Tests cover both policies | Broken link resolution if policy mismatched |
| A7 | Bridge schema major version must match for compatibility | `is_bridge_schema_compatible()` checks major version | Bridge tests verify | Swift client rejects responses |
| A8 | Generated fixtures are deterministic given same seed | `fixtures.sh` uses seed-based generation | Run twice with same seed, diff output | Flaky benchmarks/snapshot tests |
| A9 | No concurrent modification of vault files during indexing | No file locking during scan/index | Race conditions possible with external editors | Stale or inconsistent index |
| A10 | Graph algorithms terminate for any vault topology | BFS with `visited` sets and depth/node limits | `--depth` and `--limit` flags bound traversal | Infinite loops (prevented by visited sets) |

---

## G) Risk Register

| # | Risk | Likelihood | Impact | Mitigation | Owner |
|---|---|---|---|---|---|
| R1 | YAML bomb via malicious frontmatter causes OOM | Low (requires adversarial input) | High (process crash) | Add frontmatter size limit before deserialization (F2) | SDK team |
| R2 | Large vault (50k+ notes) causes graph OOM from `list_all()` | Medium (power users) | High (CLI/app crash) | Implement streaming/lazy graph loading (F1) | SDK team |
| R3 | `legacy.rs` monolith becomes untenable for review/contribution | High (already 6110 lines) | Medium (velocity loss) | Complete planned modularization (F3) | Maintainer |
| R4 | Stale daemon socket blocks CLI operations | Low (handled by CLI-004) | Medium (UX friction) | Already mitigated by stale detection; monitor effectiveness | CLI team |
| R5 | SQLite busy timeout (5s) exceeded under heavy daemon use | Low (single-user tool) | Medium (query failures) | Current timeout is adequate; monitor if daemon adoption increases | Infra |
| R6 | Specification drift between CLAUDE.md and implementation | Medium (already occurring with aliases) | Low (confusion) | Add automated spec-vs-CLI parity check (F9) | Maintainer |
| R7 | Dependency supply chain compromise (161 transitive deps) | Low (all from crates.io) | High (code execution) | Run `cargo audit` in CI; pin exact versions in lockfile (already pinned) | Infra |
| R8 | Non-UTF-8 files in vault cause panics | Low (rare in markdown vaults) | Medium (index failure) | Add `fs::read()` with lossy UTF-8 conversion fallback | SDK team |

---

## H) Prioritized Action Plan

### Must Fix (highest risk / ROI)

1. **[F2] Add frontmatter size limit** — Gate `extract_front_matter()` to reject YAML content exceeding 64KB before passing to `serde_yaml::from_str()`. Quick win, eliminates YAML bomb vector. *Effort: Quick win*

2. **[F9] Reconcile spec drift** — Update CLAUDE.md §10 to remove "compatibility aliases remain supported" claim, since CLI-001 removed them. *Effort: Quick win*

3. **[F8] Fix `apply_initial_schema` migration recording** — Either record applied migrations or remove this fast-path function to avoid indefinite re-application. *Effort: Quick win*

### Should Improve (significant value)

4. **[F1] Implement bounded graph loading** — Replace `list_all()` calls in graph operations with cursor-based or lazy loading patterns. Start with `walk` (most commonly used). *Effort: Large*

5. **[F3] Complete monolith decomposition** — Split `legacy.rs` into per-service modules. The service boundaries are already clean; this is a mechanical refactor. *Effort: Large*

6. **[F4] Push property filtering to SQL** — Replace in-memory `contains()` filter with SQL `LIKE` clause. Immediate latency improvement for large vaults. *Effort: Medium*

7. **[F5] Optimize cache invalidation digest** — Replace full-table scan with `MAX(updated_at)` check. *Effort: Medium*

8. **[F11] Add fuzz testing** — Create `cargo-fuzz` targets for markdown parsing, YAML extraction, wikilink tokenization, and `.base` parsing. *Effort: Medium*

9. **[F6] Add targeted base lookup query** — Replace `list_all().find_map()` with SQL `WHERE id = ?1 OR path = ?1`. *Effort: Quick win*

### Nice to Have (polish)

10. **[F15] Enable structured tracing** — Add `tracing::info_span!` for index pipeline, query execution, and daemon lifecycle. *Effort: Medium*

11. **[F12] Remove redundant indexes** — Drop `idx_files_match_key` and `idx_tasks_file_line`. *Effort: Quick win*

12. **[F13] Optimize graph algorithm allocations** — Use integer-indexed nodes instead of string cloning. *Effort: Medium*

13. **[F14] Parallelize ingest pipeline** — Add `rayon::par_iter()` to file parsing stage. *Effort: Medium*

14. **[F17] Deduplicate checksum loader functions** — Use generic parameter to unify `load_applied_checksums` variants. *Effort: Quick win*

15. **[F16] Restrict `BridgeKernel.connection` visibility** — Change to `pub(crate)`. *Effort: Quick win*

16. **[F18] Remove redundant `bridge_events` index** — Drop `idx_bridge_events_id`. *Effort: Quick win*

---

*End of audit report.*
