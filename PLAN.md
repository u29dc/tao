Date: March 3, 2026
Updated architecture directive: Swift native UI from the beginning; all core logic in pure Rust SDK.

## 1) Decision Update

- Core domain engine must be pure Rust.
- Swift is UI layer only.
- Swift<->Rust connection must be extremely efficient.
- CLI is a minimal agent-native wrapper over SDK primitives.
- Future TUI is a separate package on top of the same SDK.

## 2) Hard Constraints and Standards Contract

### 2.1 Product and scope constraints

- v1 includes vault mapping, markdown render, wiki links, front matter/properties, file navigation, and Bases-compatible table view.
- v1 excludes sync, plugins, theme systems, and cross-platform parity.
- UX target is Bear-level simplicity + Craft-level interaction polish + Obsidian philosophy.

### 2.2 Standards inherited from your operating rules and existing repos

From local standards and project baselines:

- `util:*` script namespace and full-gate checks via `bun run util:check` are standard. [L01][L02][L03][L07]
- Commitlint must enforce scoped conventional commits with lowercase subject and 100-char limits. [L05][L09]
- Rust workspace policy in your active Rust repos forbids unsafe code (`unsafe_code = "forbid"`). [L04][L08]
- Workspace shape in your Rust repos is SDK + CLI + TUI separation. [L04][L06]
- `unsafe_code = "forbid"`. [L04][L08]

## 3) Source-Backed Feasibility Summary

### 3.1 Obsidian compatibility feasibility

Feasibility is high for your target subset because Obsidian behavior is documented enough for a compatibility contract:

- File formats and vault semantics are documented (`.md`, `.base`, `.canvas`, attachments). [E01]
- Internal link rules (wiki links, heading links, block refs, extension handling for non-markdown files) are documented. [E02]
- Properties/front matter behavior is documented, including type semantics and YAML constraints. [E03]
- Bases has formal syntax and view docs; table view is clearly specified. [E04][E05][E06]
- Metadata cache concept is documented, aligning with index-first architecture. [E08]

### 3.2 Swift-first shell feasibility

Swift-first is feasible and aligned with your quality bar:

- AppKit gives mature tree/table surfaces via `NSOutlineView` and related controls for large hierarchies. [E16][E17]
- SwiftUI performance work is first-party documented with profiling guidance in Instruments. [E18]
- TextKit 2 is designed for modern text interactions with viewport-oriented improvements (important for large documents and editor responsiveness). [E20]
- Accessibility and reduced-motion constraints are well defined by Apple and can be treated as hard UI acceptance criteria. [E19]

### 3.3 Rust core feasibility

Core stack components are mature:

- SQLite WAL + FTS5 are first-class and appropriate for local index + search. [E21][E22]
- SQLite pragmas provide deterministic tuning points for durability/performance trade-offs. [E23]
- `rusqlite` is mature for embedded SQLite integration. [E26]
- `notify` exposes macOS filesystem watcher support through FSEvents backends. [E25]
- Rust linting supports strict unsafe enforcement strategy. [E24]

### 3.4 Swift<->Rust boundary feasibility

Two feasible integration modes:

- Primary: direct in-process FFI with UniFFI-generated Swift bindings. [E27][E28]
- Fallback: sidecar daemon IPC when FFI policy constraints cannot be met.

Given your "extremely efficient connection" requirement, primary mode is direct FFI from Swift to Rust SDK.

## 4) Final Architecture Decision

### 4.1 Chosen architecture

- Rust workspace is system core.
- Swift app is native UI shell.
- Swift calls Rust via an SDK bridge crate.
- CLI is a thin Rust wrapper around SDK service interfaces.
- TUI (later) is a Rust package over SDK read/write service APIs.

### 4.2 Why this architecture fits your goals

- Maximizes native macOS UX quality from day one.
- Keeps all correctness-critical semantics in one Rust codebase.
- Avoids duplicating domain logic across UI surfaces.
- Preserves agent-native composability via a minimal CLI contract.

### 4.3 Non-negotiable architecture invariants

- Single source of truth for domain logic is `obs-sdk` Rust crates.
- UI layers cannot implement independent parsing or resolution logic.
- Every mutating operation is SDK-mediated and auditable.
- CLI and future TUI share the same SDK calls used by Swift app.

## 5) Full Architecture Map

### 5.1 System context map

```text
Vault Files (Markdown + .base + assets)
            |
            v
      Rust SDK Workspace
   (index, parse, links, db, query)
      /            |            \
     /             |             \
Swift macOS App    CLI Wrapper    TUI (future)
(native UI)        (agent-native) (rust package)
```

### 5.2 Container/process map

```text
Process A: obs-app (Swift, macOS native)
  - AppKit/SwiftUI shell
  - calls Rust bridge APIs

Process A embedded Rust components (linked)
  - obs-sdk-core
  - obs-sdk-index
  - obs-sdk-storage
  - obs-sdk-search
  - obs-sdk-bases
  - obs-sdk-service
  - obs-sdk-bridge (Swift binding surface)

Process B: obs-cli (Rust)
  - clap command surface
  - one-envelope JSON output contract
  - invokes same obs-sdk-service APIs

Process C (later): obs-tui (Rust + Ratatui)
  - consumes obs-sdk-service read models
```

### 5.3 Workspace map

```text
obs/
  Cargo.toml
  package.json
  commitlint.config.js
  lint-staged.config.js
  crates/
    obs-sdk-core/
    obs-sdk-vault/
    obs-sdk-markdown/
    obs-sdk-links/
    obs-sdk-properties/
    obs-sdk-bases/
    obs-sdk-storage/
    obs-sdk-search/
    obs-sdk-watch/
    obs-sdk-service/
    obs-sdk-bridge/
    obs-cli/
    obs-tui/                  # later phase
  apps/
    obs-macos/                # Xcode project
```

### 5.4 Component map: SDK internals

```text
obs-sdk-service
  -> orchestrates domain operations and transactions
  -> depends on:
       obs-sdk-vault       (path canonicalization, file IO policy)
       obs-sdk-markdown    (markdown parse/render pipeline)
       obs-sdk-links       (wikilink parse + deterministic resolver)
       obs-sdk-properties  (front matter parse + typed projection)
       obs-sdk-bases       (.base parser + query planning)
       obs-sdk-storage     (sqlite schema, migrations, query adapters)
       obs-sdk-search      (fts indexing + search ranking)
       obs-sdk-watch       (fs event normalization + reconciliation)
```

### 5.5 Data model map (SQLite)

Core schema (v1):

- `files(id, path, ext, mtime_ns, size, content_hash, title, is_deleted)`
- `documents(file_id, markdown_raw, markdown_rendered_html, render_hash)`
- `frontmatter(file_id, raw_yaml, parse_ok, parse_error)`
- `properties(file_id, key, value_kind, value_text, value_num, value_bool, value_date, value_json)`
- `links(id, src_file_id, raw_target, resolved_file_id, target_heading, target_block, display_text, kind)`
- `headings(file_id, level, text, slug, byte_start, byte_end)`
- `blocks(file_id, block_id, byte_start, byte_end)`
- `bases(id, path, raw_yaml, parse_ok, parse_error)`
- `base_views(base_id, name, kind, config_json)`
- `fts_documents` (FTS5 virtual table)
- `index_state(key, value)`

SQLite sources: WAL, FTS5, pragma tunables. [E21][E22][E23]

### 5.6 Runtime event map

```text
File change detected
  -> normalize event batch
  -> identify candidate files by path
  -> mtime/size fast check
  -> content hash check for changed candidates
  -> parse markdown/front matter/links/headings/blocks
  -> update sqlite in single transaction
  -> emit typed domain events
  -> UI subscriptions update visible views
```

FSEvents and coalescing behavior must be handled with reconciliation logic. [E15]

### 5.7 SDK service API map (language-neutral)

Read APIs:

- `vault_open(path)`
- `vault_stats()`
- `note_get(path)`
- `note_render(path)`
- `note_properties(path)`
- `note_backlinks(path)`
- `search_query(query, options)`
- `bases_list()`
- `base_table(path_or_id, view_name)`

Write APIs:

- `note_create(path, content, frontmatter)`
- `note_update(path, patch_or_full)`
- `note_rename(from, to)`
- `note_delete(path, soft)`
- `property_set(path, key, value)`
- `base_update(path, yaml_patch)`

System APIs:

- `index_rebuild(mode)`
- `index_reconcile()`
- `watch_start()`
- `watch_stop()`
- `health_snapshot()`

### 5.8 Error and contract map

SDK error envelope shape:

- `code` (stable string)
- `message` (human text)
- `context` (structured object)
- `hint` (optional remediation)

CLI JSON mode must print one compact envelope to stdout, with diagnostics on stderr (pattern aligned with your existing agent-native repos). [L06]

### 5.9 Concurrency map

- UI thread never blocks on file parsing or database writes.
- SDK uses async orchestration + bounded worker pools.
- Write operations are serialized per vault via transaction queue.
- Read operations are lock-minimized and cache-first.

### 5.10 Caching map

- L1 in-memory caches: path->file metadata, file_id->outgoing links, file_id->properties map.
- L2 SQLite persisted derived state.
- L3 render cache keyed by content hash.

### 5.11 Security and safety map

- No network exfiltration in core paths unless explicitly enabled.
- Vault path sandboxing: operations constrained to configured vault root.
- Secrets in env or keychain only; never in repo.
- Rust unsafe policy: enforce `unsafe_code = "forbid"` in workspace lint config. [L04][L08][E24]

## 6) Swift<->Rust Connection Design (Efficient by Default)

### 6.1 Primary mode: direct FFI bridge

Implement `obs-sdk-bridge` using UniFFI to generate Swift bindings over stable Rust service interfaces. [E27][E28]

Design rules:

- Bridge crate exports coarse-grained calls, not chatty per-token methods.
- Return typed DTOs, not raw JSON strings.
- Support batch APIs for tree loading, backlink pages, and table chunks.
- Push events using callback/subscription channels for index updates.

### 6.2 Performance guardrails for bridge design

- Minimize cross-boundary call count by batching.
- Avoid large string copies where not needed.
- Move expensive parse/query work fully into Rust.
- Expose pagination/windowing in every potentially large list API.

### 6.3 Fallback mode: sidecar IPC (contingency only)

If no-unsafe constraints conflict with generated FFI scaffolding in your policy, provide sidecar mode:

- Swift app talks to local SDK daemon over Unix domain socket.
- Same service interface and DTO contracts.
- Default remains direct FFI unless policy blocks it.

## 7) Swift App Architecture (Native UI Layer)

### 7.1 UI stack

- App shell: SwiftUI for high-level composition.
- Heavy hierarchy views: AppKit-backed components where required.
- File tree and table-heavy screens can use `NSOutlineView`/table bridges for consistent performance at scale. [E17]

### 7.2 View model boundaries

- View models request read models from Rust SDK.
- View models do not perform parsing, link resolution, or property inference.
- Mutation flows call SDK methods and render optimistic updates with rollback on typed error.

### 7.3 Editor and rendering strategy

- v1 can be read-first with lightweight editing.
- For high-fidelity editing, use TextKit 2-based editor surface.
- Markdown render path remains Rust-owned for deterministic output.

TextKit 2 direction source: WWDC and Apple docs. [E20]

### 7.4 Motion and accessibility

- Respect reduced motion preference by default.
- Avoid layout-heavy animation paths.
- Ensure keyboard-first navigation and focus visibility.

Apple accessibility source: reduced-motion criteria. [E19]

## 8) CLI Architecture (Minimal Agent-Native Wrapper)

### 8.1 CLI role

CLI is not a second application logic stack. It is a thin command adapter over SDK service calls.

### 8.2 Command contract

Core command groups:

- `obs health --json`
- `obs tools --json`
- `obs vault {open|stats|reindex|reconcile} --json`
- `obs note {get|render|create|update|rename|delete} --json`
- `obs links {outgoing|backlinks|resolve} --json`
- `obs props {get|set|list} --json`
- `obs bases {list|view|validate} --json`
- `obs search query --json`

### 8.3 CLI envelope rules

- Exactly one JSON envelope object to stdout in `--json` mode.
- Stable exit codes.
- Tracing and diagnostics to stderr.

Pattern based on your existing CLI standards. [L06]

## 9) Future TUI Package Plan

### 9.1 TUI scope

- Route-based terminal UI for high-speed navigation/search/properties/table.
- No duplicated domain logic.
- Uses SDK service read models and mutations directly.

### 9.2 TUI stack

- `ratatui` + `crossterm` (same pattern as your existing Rust TUI projects). [L06][E29]

### 9.3 TUI timing

- Start after SDK and Swift shell stabilize.
- TUI is Phase 6+ deliverable, not v1 blocker.

## 10) Engineering Policy Baseline for This Project

### 10.1 Rust policy

- Edition: 2024.
- Workspace split: sdk, cli, bridge, optional tui.
- Enforce `unsafe_code = "forbid"` in workspace lints unless explicitly waived for generated bridge code in isolated crate.
- Clippy warnings are errors in CI.

Policy alignment references: your repos + rust lint docs. [L04][L08][E24]

### 10.2 JS/tooling policy

- Use Bun for script orchestration and hooks.
- Keep `util:*` scripts and `prepare` hook.
- Run full gate via lint-staged on all changed files.

Policy references: align + cho/fin patterns. [L01][L02][L03][L07]

### 10.3 Commit policy

- Conventional commits with required scope.
- Lowercase subject.
- 100-char line limits.

References: commitlint configs from your repos. [L05][L09]

## 11) Build and Quality Gate Blueprint

### 11.1 Proposed repo scripts

Target scripts (pattern-aligned with your repos):

- `prepare`: `husky`
- `build`: release build for workspace binaries + install copy targets
- `util:format`: `cargo fmt --all`
- `util:lint`: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `util:types`: `cargo check --workspace`
- `util:test`: `cargo test --workspace`
- `util:build`: `cargo build --workspace`
- `util:audit`: `cargo audit`
- `util:check`: full chained gate

Pattern source: `cho` and `fin` scripts. [L03][L07]

### 11.2 CI gates (required for merge)

- Zero format drift.
- Zero clippy warnings.
- Zero compile errors.
- All tests pass.
- Build passes for SDK + CLI + bridge.
- Swift app build passes in CI runner.

## 12) Performance Program

### 12.1 Target budgets

Define budgets as engineering constraints, not aspirational goals:

- App interactive (warm): <= 300 ms.
- App interactive (cold): <= 900 ms.
- Open note p50: <= 30 ms.
- Open note p95: <= 120 ms.
- Incremental index apply p50: <= 120 ms.
- Search p50: <= 15 ms.

### 12.2 Measurement harness

- Rust microbench suite for parse, link resolve, property parse, bases query, search query.
- Integration bench over synthetic vault sets: 1k, 5k, 10k, 25k notes.
- Swift UI trace profiles for startup/open/scroll/search.

### 12.3 Tuning controls

SQLite controls (example baseline):

- WAL enabled.
- tuned `synchronous`, `cache_size`, `mmap_size` based on benchmark profile.

Sources for tuning controls: SQLite docs. [E21][E23]

## 13) Risks, Unknowns, and Mitigations

1. FFI policy conflict with strict no-unsafe expectations.

- Mitigation: keep core crates strict; isolate bridge generation; fallback to IPC mode if needed.

2. Obsidian edge-case drift in links/properties.

- Mitigation: explicit compatibility corpus from official docs and real vault snapshots.

3. Bases syntax evolution changes behavior.

- Mitigation: parser versioning and strict validation path per `.base` revision.

4. Large vault watcher event loss/coalescing.

- Mitigation: periodic reconciliation scans in addition to event stream.

5. Scope creep into plugins/sync before core quality is stable.

- Mitigation: lock v1 scope and enforce phase exit criteria.

## 14) Architecture Exit Criteria and Decision Gates

Gate A (end Phase 1):

- SDK parses and indexes real vault correctly.
- Link resolution deterministic across repeated runs.
- Properties parse success >= 99.5 percent on corpus.

Gate B (end Phase 3):

- Swift shell navigation and read path meet p95 budgets.
- No critical accessibility defects.

Gate C (end Phase 5):

- Bases table workflows stable on production vault.
- CLI JSON contracts validated for agent use.

Gate D (pre-v1 release):

- Full quality gate green.
- Crash recovery and index self-heal validated.
- Documentation and runbooks complete.

## 15) Full Development Roadmap (Linear Ticket Ledger)

Format:

- `ID`: ticket id
- `Depends`: hard dependency ids
- `DoD`: definition of done (merge criteria)

### 15.1 Documentation tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| DOC-001 | Create architecture ADR index | - | `docs/adr/README.md` with numbering and template |
| DOC-002 | Write product scope contract v1 | DOC-001 | Scope doc lists in/out items and acceptance criteria |
| DOC-003 | Write Obsidian compatibility matrix | DOC-001 | Matrix covers links, properties, bases, markdown subset |
| DOC-004 | Write SDK API surface spec | DOC-001 | Typed interface doc for all read/write/system APIs |
| DOC-005 | Write error code catalog | DOC-004 | Stable error code table with remediation hints |
| DOC-006 | Write performance budget contract | DOC-001 | Budget doc checked into repo and referenced by CI |

### 15.2 Architecture tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| ARC-001 | Define workspace crate boundaries | DOC-002 | `Cargo.toml` workspace members and crate docs committed |
| ARC-002 | Define Swift app module boundaries | DOC-002 | Xcode project module map documented |
| ARC-003 | Define bridge DTO contracts | DOC-004 | Shared DTO schema and versioning policy committed |
| ARC-004 | Define event model for index updates | ARC-003 | Event types and payload contracts documented |
| ARC-005 | Define resolver tie-break algorithm | DOC-003 | Deterministic resolver spec with ambiguity policy |
| ARC-006 | Define migration/versioning strategy | ARC-001 | DB schema semver + migration policy documented |

### 15.3 Infrastructure tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| INFRA-001 | Bootstrap Rust workspace skeleton | ARC-001 | All crates compile with placeholder code |
| INFRA-002 | Add workspace lint policy | INFRA-001 | `unsafe_code = "forbid"` and lint config active |
| INFRA-003 | Add root package scripts and hooks | INFRA-001 | `package.json`, husky, lint-staged, commitlint aligned |
| INFRA-004 | Add CI workflow for Rust gates | INFRA-003 | CI runs format/lint/check/test/build |
| INFRA-005 | Add CI workflow for Swift build | ARC-002 | Swift target build/test job passes |
| INFRA-006 | Add cargo audit gate | INFRA-004 | security audit job integrated in CI |
| INFRA-007 | Add benchmark harness scaffold | DOC-006 | bench crate + baseline datasets checked in |
| INFRA-008 | Add release profile tuning | INFRA-001 | release profile matches performance policy |

### 15.4 SDK tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| SDK-001 | Implement path canonicalization service | INFRA-001 | Handles unicode, symlink, case policy tests |
| SDK-002 | Implement vault scan service | SDK-001 | Full scan returns normalized file manifest |
| SDK-003 | Implement file fingerprint service | SDK-002 | mtime/size/hash fingerprint logic tested |
| SDK-004 | Implement markdown ingest pipeline shell | SDK-002 | Parser entrypoints and pipeline orchestrator compile |
| SDK-005 | Implement rendered html cache policy | SDK-004 | hash-keyed render cache with eviction policy |
| SDK-006 | Implement SDK transaction coordinator | DB-004 | write operations use typed transaction API |
| SDK-007 | Implement domain event bus | ARC-004 | publish/subscribe event delivery tested |
| SDK-008 | Implement note create/update/delete services | SDK-006 | CRUD flows pass integration tests |
| SDK-009 | Implement note rename/move services | SDK-008 | path + link re-resolution integration tests |
| SDK-010 | Implement health snapshot service | SDK-002 | reports index lag, db status, watcher status |
| SDK-011 | Implement import/export service boundaries | SDK-008 | clear interfaces for future import jobs |
| SDK-012 | Implement failure-safe rollback paths | SDK-006 | failed writes rollback with no index corruption |
| SDK-013 | Implement idempotent reconcile service | SDK-003 | repeated reconcile yields stable state |
| SDK-014 | Implement service-level tracing hooks | INFRA-004 | structured traces with correlation ids |
| SDK-015 | Implement SDK config loader | INFRA-001 | config precedence and validation tests |
| SDK-016 | Implement SDK feature flag registry | SDK-015 | toggles for experimental modules |

### 15.5 Database tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| DB-001 | Create initial sqlite schema | ARC-006 | schema migration `0001_init.sql` applied cleanly |
| DB-002 | Create migration runner | DB-001 | forward-only migration runner with checksum guard |
| DB-003 | Implement files table repository | DB-001 | CRUD + bulk upsert tested |
| DB-004 | Implement transaction wrapper | DB-001 | typed transaction API used by services |
| DB-005 | Implement links/properties repositories | DB-003 | join queries validated by tests |
| DB-006 | Implement bases repositories | DB-003 | `.base` metadata read/write queries tested |
| DB-007 | Implement render cache repositories | DB-003 | rendered html persistence tested |
| DB-008 | Implement index state repositories | DB-003 | state keys for checkpoint/reconcile persisted |

### 15.6 Indexing tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| IDX-001 | Implement initial full indexing workflow | SDK-004, DB-005 | full vault index populates all core tables |
| IDX-002 | Implement incremental indexing workflow | IDX-001, SDK-003 | single-file updates touch only impacted rows |
| IDX-003 | Implement coalesced batch apply | IDX-002 | burst edits processed in bounded batches |
| IDX-004 | Implement stale record cleanup | IDX-002 | deleted/moved files removed from index |
| IDX-005 | Implement checkpointed index progress | DB-008 | restart resumes without full rebuild |
| IDX-006 | Implement reconciliation scanner | IDX-002 | drift scan repairs missed watcher events |
| IDX-007 | Add index consistency checker | IDX-001 | checker reports orphan rows and broken refs |
| IDX-008 | Add index self-heal command | IDX-007 | command repairs common inconsistencies |

### 15.7 Link resolver tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| LINK-001 | Implement wikilink parser | SDK-004 | parses target/display/heading/block forms |
| LINK-002 | Implement deterministic path resolver | ARC-005, LINK-001 | ties resolved by documented algorithm |
| LINK-003 | Implement heading target resolver | LINK-001 | heading links resolve to indexed headings |
| LINK-004 | Implement block target resolver | LINK-001 | block refs resolve to indexed blocks |
| LINK-005 | Implement unresolved link tracking | LINK-002 | unresolved links persisted and queryable |
| LINK-006 | Implement backlink graph service | LINK-002 | outgoing/backlink queries return stable order |

### 15.8 Properties tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| PROP-001 | Implement front matter extraction | SDK-004 | YAML block extraction with parse errors captured |
| PROP-002 | Implement typed property projection | PROP-001 | bool/number/date/list types normalized |
| PROP-003 | Implement property update service | SDK-008, PROP-002 | typed set operations persist and re-render |
| PROP-004 | Implement default property mappings | PROP-002 | tags/aliases/css classes mapped consistently |
| PROP-005 | Implement malformed YAML tolerance | PROP-001 | malformed docs do not crash indexing |
| PROP-006 | Implement property query API | PROP-002 | filter/sort APIs for bases and cli |

### 15.9 Bases tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| BASE-001 | Implement `.base` file parser | DB-006 | valid syntax parsed into typed model |
| BASE-002 | Implement base view registry | BASE-001 | list views with type and config |
| BASE-003 | Implement table query planner | BASE-001, PROP-006 | table view query compiles to db query plan |
| BASE-004 | Implement table row executor | BASE-003 | rows returned with paging and sort |
| BASE-005 | Implement table column config support | BASE-004 | column order/visibility persisted |
| BASE-006 | Implement basic summaries | BASE-004 | count/min/max/avg summary rows supported |
| BASE-007 | Implement base validation API | BASE-001 | clear diagnostics for invalid base config |
| BASE-008 | Implement base refresh invalidation | BASE-004 | base results update on related metadata changes |

### 15.10 Bridge tickets (Swift<->Rust)

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| FFI-001 | Create bridge crate and export shell | ARC-003, INFRA-001 | bridge crate compiles and exports minimal API |
| FFI-002 | Generate Swift bindings for read APIs | FFI-001, SDK-010 | Swift calls `vault_stats` and `note_get` successfully |
| FFI-003 | Generate Swift bindings for write APIs | FFI-002, SDK-009 | Swift write calls update SDK state safely |
| FFI-004 | Implement DTO versioning strategy | ARC-003 | versioned DTOs with backward-compat checks |
| FFI-005 | Implement bridge error mapping | DOC-005, FFI-002 | Rust errors map to Swift typed errors |
| FFI-006 | Implement event subscription bridge | SDK-007, FFI-002 | Swift receives index/update events |
| FFI-007 | Implement batch/list windowing APIs | FFI-002 | large lists exposed via paged endpoints |
| FFI-008 | Add bridge performance benchmarks | INFRA-007, FFI-006 | boundary call budgets tracked in CI report |

### 15.11 Swift app tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| APP-001 | Create macOS app shell project | ARC-002 | app launches with split layout scaffolding |
| APP-002 | Integrate bridge package into app build | FFI-002 | app links and calls rust read APIs |
| APP-003 | Implement vault picker and open flow | APP-002 | opens vault and shows root state |
| APP-004 | Implement file tree view model | APP-003 | lazy loaded tree navigation works |
| APP-005 | Implement note reading pane | APP-003 | selected note renders markdown |
| APP-006 | Implement properties pane | APP-005, FFI-003 | properties display and edits persist |
| APP-007 | Implement backlinks pane | APP-005 | outgoing and backlink panels render |
| APP-008 | Implement quick-open command palette | APP-004 | keyboard command palette opens note by search |
| APP-009 | Implement bases table screen | BASE-004, APP-003 | table view loads and paginates |
| APP-010 | Implement app-level error handling UI | FFI-005 | typed errors displayed with recovery actions |
| APP-011 | Implement reduced-motion behavior | APP-001 | UI honors reduced motion settings |
| APP-012 | Implement startup state restoration | APP-003 | restores last vault and last note safely |

### 15.12 CLI tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| CLI-001 | Scaffold CLI crate and command tree | INFRA-001 | `obs --help` with grouped commands |
| CLI-002 | Implement one-envelope JSON output | CLI-001, DOC-005 | `--json` prints one object to stdout |
| CLI-003 | Add vault command group wrappers | CLI-001, SDK-010 | open/stats/reindex/reconcile commands work |
| CLI-004 | Add note command group wrappers | CLI-001, SDK-009 | read/write commands map to sdk calls |
| CLI-005 | Add links and properties wrappers | CLI-001, LINK-006, PROP-006 | link/property commands operate on vault |
| CLI-006 | Add bases/search wrappers | CLI-001, BASE-008 | bases view and search command outputs stable JSON |

### 15.13 TUI tickets (future)

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| TUI-001 | Scaffold TUI crate | INFRA-001 | terminal app starts with placeholder route |
| TUI-002 | Implement route shell and keymap | TUI-001 | route switching and command palette work |
| TUI-003 | Implement note list and viewer route | TUI-002, SDK-010 | view notes and note content |
| TUI-004 | Implement search route | TUI-002, BASE-008 | search list and open note flow work |
| TUI-005 | Implement bases table route | TUI-002, BASE-004 | table route paginates and sorts |

### 15.14 QA and validation tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| QA-001 | Build conformance fixture vault | DOC-003 | fixture set covers links/properties/bases edge cases |
| QA-002 | Add SDK integration test harness | INFRA-004 | integration tests run fixture vault end-to-end |
| QA-003 | Add resolver determinism tests | LINK-002, QA-001 | repeated runs yield identical resolutions |
| QA-004 | Add malformed front matter tests | PROP-005, QA-001 | malformed docs handled without crash |
| QA-005 | Add bases parser and table tests | BASE-008, QA-001 | table outputs match expected snapshots |
| QA-006 | Add watcher/reconcile chaos tests | IDX-006 | simulated burst changes remain consistent |
| QA-007 | Add Swift UI smoke tests | APP-012 | launch/open/navigate/edit smoke suite passes |
| QA-008 | Add CLI JSON contract tests | CLI-006 | schema validations for all `--json` commands |

### 15.15 Performance tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| PERF-001 | Capture baseline benchmarks on 1k vault | INFRA-007 | baseline report committed |
| PERF-002 | Capture baseline benchmarks on 5k vault | PERF-001 | report includes p50/p95 latencies |
| PERF-003 | Capture baseline benchmarks on 10k vault | PERF-002 | report includes memory and startup metrics |
| PERF-004 | Tune sqlite pragmas for workload | DB-008, PERF-003 | selected pragma profile documented |
| PERF-005 | Optimize link resolution hot paths | LINK-006, PERF-003 | resolver throughput improves vs baseline |
| PERF-006 | Optimize bridge call batching | FFI-008, PERF-003 | boundary call count reduced in key flows |
| PERF-007 | Optimize app startup pipeline | APP-012, PERF-003 | startup p95 meets budget |
| PERF-008 | Lock perf budgets in CI gate | DOC-006, PERF-007 | CI fails on budget regressions |

### 15.16 Release tickets

| ID | Title | Depends | DoD |
| --- | --- | --- | --- |
| REL-001 | Define versioning policy for sdk/cli/app | ARC-006 | semver + compatibility rules documented |
| REL-002 | Create release checklist and runbook | DOC-001 | runbook includes rollback plan |
| REL-003 | Package CLI install artifact flow | CLI-006 | release build installs binary to target path |
| REL-004 | Package macOS app artifact flow | APP-012 | signed app artifact produced in CI |
| REL-005 | Add migration preflight checks | DB-002 | release validates schema migration before startup |
| REL-006 | Cut v1.0.0 release candidate | REL-001, REL-002, REL-003, REL-004, REL-005, QA-008, PERF-008 | RC build and acceptance report published |

## 16) Phase Plan Mapped to Ticket IDs

### Phase 0 - Foundation and contracts

- DOC-001..DOC-006
- ARC-001..ARC-006
- INFRA-001..INFRA-008

Exit condition:

- workspace builds, policy gates wired, architecture contracts frozen.

### Phase 1 - Core SDK vertical slice

- SDK-001..SDK-010
- DB-001..DB-005
- IDX-001..IDX-004
- LINK-001..LINK-003
- PROP-001..PROP-003

Exit condition:

- open vault, index, read note, link/backlink query, property read/write.

### Phase 2 - Bases and completeness

- SDK-011..SDK-016
- DB-006..DB-008
- IDX-005..IDX-008
- LINK-004..LINK-006
- PROP-004..PROP-006
- BASE-001..BASE-008

Exit condition:

- stable metadata-driven bases table workflows on real vault.

### Phase 3 - Bridge and native app

- FFI-001..FFI-008
- APP-001..APP-008

Exit condition:

- Swift app uses Rust SDK for core read/write paths with event updates.

### Phase 4 - Product polish and command surface

- APP-009..APP-012
- CLI-001..CLI-006
- QA-001..QA-005
- PERF-001..PERF-005

Exit condition:

- daily-driver readiness for your target feature set.

### Phase 5 - hardening and release

- QA-006..QA-008
- PERF-006..PERF-008
- REL-001..REL-006

Exit condition:

- v1 candidate shipped with green quality/perf gates.

### Phase 6 - optional TUI package

- TUI-001..TUI-005

Exit condition:

- terminal workflows available without new domain logic.

## 17) Suggested Commit Scope Taxonomy

Recommended scopes (strict):

- `sdk`, `db`, `index`, `links`, `props`, `bases`, `bridge`, `app`, `cli`, `tui`, `perf`, `docs`, `config`, `deps`, `ci`, `release`

Example commits:

- `feat(sdk): add deterministic wikilink resolver`
- `fix(app): prevent tree reload on note pane focus change`
- `test(bases): add table summary fixtures for mixed property types`

## 18) Source Index

### External sources

- [E01] Obsidian file formats: https://help.obsidian.md/file-formats
- [E02] Obsidian internal links: https://help.obsidian.md/Linking%20notes%20and%20files/Internal%20links
- [E03] Obsidian properties: https://help.obsidian.md/properties
- [E04] Obsidian bases: https://help.obsidian.md/bases
- [E05] Obsidian bases syntax: https://help.obsidian.md/bases/syntax
- [E06] Obsidian bases table view: https://help.obsidian.md/bases/views/table
- [E07] Obsidian aliases: https://help.obsidian.md/aliases
- [E08] Obsidian data storage + metadata cache: https://help.obsidian.md/data-storage
- [E09] Obsidian developer blog performance note (quick switcher threshold): https://obsidian.md/blog/newsletter/2022-05-10-announcing-obsidian-v0.14.6/
- [E10] Tauri architecture: https://v2.tauri.app/concept/architecture/
- [E11] Tauri process model: https://v2.tauri.app/concept/process-model/
- [E12] Tauri IPC: https://v2.tauri.app/concept/inter-process-communication/
- [E13] Tauri size profile: https://v2.tauri.app/concept/size/
- [E14] Electron docs: https://www.electronjs.org/docs/latest
- [E15] Apple FSEvents guide: https://developer.apple.com/library/archive/documentation/Darwin/Conceptual/FSEvents_ProgGuide/TechnologyOverview/TechnologyOverview.html
- [E16] Apple `NSOutlineView`: https://developer.apple.com/documentation/appkit/nsoutlineview
- [E17] Apple `NSOutlineViewDataSource`: https://developer.apple.com/documentation/appkit/nsoutlineviewdatasource
- [E18] Apple SwiftUI performance docs: https://developer.apple.com/documentation/Xcode/understanding-and-improving-swiftui-performance
- [E19] Apple reduced motion criteria: https://developer.apple.com/help/app-store-connect/manage-app-accessibility/reduced-motion-accessibility-evaluation-criteria
- [E20] WWDC22 TextKit 2 talk: https://developer.apple.com/videos/play/wwdc2022/10090/
- [E21] SQLite WAL: https://sqlite.org/wal.html
- [E22] SQLite FTS5: https://www.sqlite.org/fts5.html
- [E23] SQLite pragma reference: https://www.sqlite.org/pragma.html
- [E24] Rust lint listing (`unsafe_code`): https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
- [E25] notify crate docs: https://docs.rs/notify/latest/x86_64-apple-darwin/notify/index.html
- [E26] rusqlite crate docs: https://docs.rs/rusqlite/latest/rusqlite/
- [E27] UniFFI Swift overview: https://mozilla.github.io/uniffi-rs/latest/swift/overview.html
- [E28] UniFFI Swift package manager distribution: https://mozilla.github.io/uniffi-rs/latest/swift/distributing/using-swift-package-manager.html
- [E29] Ratatui docs: https://ratatui.rs/

### Local sources

- [L01] `/Users/han/Git/dot/agents/skills/align/SKILL.md`
- [L02] `/Users/han/Git/dot/agents/skills/align/references/index.md`
- [L03] `/Users/han/Git/cho/package.json`
- [L04] `/Users/han/Git/cho/Cargo.toml`
- [L05] `/Users/han/Git/cho/commitlint.config.js`
- [L06] `/Users/han/Git/fin/AGENTS.md`
- [L07] `/Users/han/Git/fin/package.json`
- [L08] `/Users/han/Git/fin/Cargo.toml`
- [L09] `/Users/han/Git/fin/commitlint.config.js`

## 19) Autonomous Execution Contract (Codex-Ready)

This section defines the minimum deterministic protocol for long-running autonomous implementation sessions.

### 19.1 Required control files

- `tickets.csv`: canonical machine-readable backlog.
- `run-state.json`: current phase, active ticket, retry counters, last completed ticket, active blockers.
- `progress.md`: append-only human-readable execution log with timestamped entries.
- `blockers.md`: active blockers with owner, impact, and unblocking condition.

If any required control file is missing, the agent must create it before implementing feature code.

### 19.2 Ticket state machine

Allowed states:

- `todo`
- `in_progress`
- `blocked`
- `done`
- `failed`

Rules:

- Exactly one ticket may be `in_progress` at a time per autonomous worker session.
- A ticket can transition from `blocked` to `in_progress` only when all blocker predicates are cleared.
- `done` is terminal.
- `failed` is terminal for the current run and must include root cause + recovery note in `progress.md`.

### 19.3 Deterministic execution order

Ticket selection algorithm:

1. Select lowest phase with unfinished tickets.
2. Within that phase, select tickets whose dependencies are all `done`.
3. Break ties by lexical ticket ID.
4. If no selectable ticket exists and unfinished tickets remain, mark run `blocked` and emit blocker summary.

### 19.4 Retry and escalation policy

- Default `max_retries` per ticket is `3`.
- On failure:
  - increment retry counter
  - write failure summary + stack trace excerpt + attempted fix in `progress.md`
  - if retries remain, return ticket to `todo`
  - if retries exhausted, set ticket to `failed`, add blocker entry, continue with other unblocked tickets
- Never silently skip failed tickets.

### 19.5 Evidence requirements for `done`

A ticket can only be set to `done` if all are true:

- Code/config/docs change exists in workspace and is relevant to ticket DoD.
- Required checks for that ticket scope passed locally.
- `progress.md` contains a one-line evidence pointer:
  - changed files
  - commands run
  - pass/fail outcomes
- Any residual risk is explicitly noted.

### 19.6 Quality gate policy

Mandatory full gate before any release milestone (`REL-*`) and at end of each phase:

- `bun run util:format`
- `bun run util:lint`
- `bun run util:types`
- `bun run util:test`
- `bun run util:build`
- `bun run util:audit` (when configured)
- `bun run util:check`

No ticket that weakens gate integrity may be merged as `done`.

### 19.7 Session checkpoint protocol

Checkpoint every 30 minutes or every ticket completion, whichever is earlier:

- persist `run-state.json`
- append progress entry
- append blockers update (if any)
- list next candidate ticket IDs

On session restart:

1. load `run-state.json`
2. verify file tree and lockfile integrity
3. resume last `in_progress` ticket or select next deterministic candidate

### 19.8 Stop conditions

Stop autonomous execution only when one of these is true:

- all non-TUI tickets are `done`
- hard blocker requires human decision
- environment/toolchain failure prevents safe continuation

When stopping, emit:

- completed ticket IDs in this run
- active blockers
- exact next ticket ID

## 20) Machine-Executable Ticket Manifest Contract

`tickets.csv` is authoritative for automation. `PLAN.md` is canonical narrative and architecture context.

### 20.1 CSV schema

Required columns:

- `id`
- `title`
- `area`
- `phase`
- `depends_on`
- `status`
- `priority`
- `owner`
- `max_retries`
- `retry_count`
- `blocked_reason`
- `dod`

Conventions:

- `depends_on` uses `|` as separator for multiple IDs.
- Empty dependency means runnable when phase is active.
- `status` defaults to `todo`.
- `owner` defaults to `unassigned`.
- `retry_count` defaults to `0`.

### 20.2 Manifest integrity rules

- Every dependency ID must exist in `tickets.csv`.
- No circular dependencies.
- Ticket IDs are immutable.
- `REL-006` must remain explicitly dependency-bound and never use informal dependency text.

### 20.3 Sync rules between PLAN and CSV

- If ticket title or DoD changes in `PLAN.md`, update `tickets.csv` in the same commit.
- If phase mapping changes in Section 16, update `phase` values in `tickets.csv` in the same commit.
- Any mismatch between `PLAN.md` and `tickets.csv` blocks autonomous execution until reconciled.
