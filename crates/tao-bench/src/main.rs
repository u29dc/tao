use std::cmp::Ordering;
use std::fs;
use std::path::Component;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use rusqlite::Connection;
use serde_json::{Value as JsonValue, json};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel};
use tao_sdk_links::LinkResolutionIndex;
use tao_sdk_markdown::{MarkdownParseRequest, MarkdownParser};
use tao_sdk_search::{SearchQueryRequest, SearchQueryService};
use tao_sdk_service::{
    BacklinkGraphService, GraphWalkRequest, SearchKind, VaultSearchRequest, VaultSearchService,
};
use tempfile::{TempDir, tempdir};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Scenario {
    Parse,
    Resolve,
    Search,
    GraphWalk,
    UnifiedQuery,
    Bridge,
    Startup,
}

#[derive(Parser, Debug)]
#[command(name = "tao-bench")]
#[command(about = "benchmark harness for tao workloads")]
struct Args {
    #[arg(long, value_enum, default_value_t = Scenario::Parse)]
    scenario: Scenario,
    #[arg(long, default_value_t = 200)]
    iterations: u64,
    #[arg(long, default_value_t = 128)]
    bridge_notes: u64,
    #[arg(long, default_value_t = 50.0)]
    max_p50_ms: f64,
    #[arg(long, default_value_t = 120.0)]
    max_p95_ms: f64,
    #[arg(long, default_value_t = false)]
    enforce_budgets: bool,
    #[arg(long)]
    json_out: Option<PathBuf>,
    #[arg(long)]
    markdown_out: Option<PathBuf>,
    #[arg(long)]
    vault_root: Option<PathBuf>,
    #[arg(long)]
    db_path: Option<PathBuf>,
    #[arg(long, default_value = "notes/projects/project-1.md")]
    graph_root: String,
    #[arg(long, default_value_t = 2)]
    graph_depth: u32,
    #[arg(long, default_value_t = 200)]
    graph_limit: u32,
    #[arg(long, default_value_t = false)]
    graph_include_folders: bool,
    #[arg(long, default_value = "project")]
    query_text: String,
    #[arg(long, default_value_t = 100)]
    query_limit: u64,
}

#[derive(Debug, Clone, Copy)]
struct LatencySummary {
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

impl LatencySummary {
    fn from_samples(mut samples: Vec<f64>) -> Result<Self> {
        if samples.is_empty() {
            bail!("benchmark produced no latency samples");
        }

        samples.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        let p50_ms = percentile(&samples, 50.0);
        let p95_ms = percentile(&samples, 95.0);
        let max_ms = samples.last().copied().unwrap_or(0.0);

        Ok(Self {
            p50_ms,
            p95_ms,
            max_ms,
        })
    }

    fn as_json(self) -> JsonValue {
        json!({
            "p50_ms": round_ms(self.p50_ms),
            "p95_ms": round_ms(self.p95_ms),
            "max_ms": round_ms(self.max_ms),
        })
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("tao-bench failed: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    match args.scenario {
        Scenario::Bridge => run_bridge_benchmark(&args),
        Scenario::GraphWalk => run_graph_walk_benchmark(&args),
        Scenario::Parse => run_parse_benchmark(&args),
        Scenario::Resolve => run_resolve_benchmark(&args),
        Scenario::Search => run_search_benchmark(&args),
        Scenario::Startup => run_startup_benchmark(&args),
        Scenario::UnifiedQuery => run_unified_query_benchmark(&args),
    }
}

fn run_parse_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("parse benchmark iterations must be greater than zero");
    }
    let (vault_root, _temp) = parse_benchmark_vault(args)?;
    let docs =
        collect_markdown_payloads(&vault_root, usize::try_from(args.bridge_notes.max(128))?)?;
    if docs.is_empty() {
        bail!(
            "parse benchmark found no markdown files under {}",
            vault_root.display()
        );
    }

    let parser = MarkdownParser;
    let mut samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut headings_total = 0_u64;
    let docs_len = u64::try_from(docs.len()).context("convert parsed document count")?;
    for iteration in 0..args.iterations {
        let index = usize::try_from(iteration % docs_len).context("convert sample index")?;
        let (path, raw) = &docs[index];
        let start = Instant::now();
        let parsed = parser
            .parse(MarkdownParseRequest {
                normalized_path: path.clone(),
                raw: raw.clone(),
            })
            .context("parse benchmark markdown sample failed")?;
        samples.push(elapsed_ms(start));
        headings_total = headings_total.saturating_add(u64::try_from(parsed.headings.len())?);
        std::hint::black_box(parsed);
    }

    let summary = LatencySummary::from_samples(samples)?;
    println!(
        "parse docs={} p50_ms={:.3} p95_ms={:.3} headings_avg={:.1}",
        docs.len(),
        summary.p50_ms,
        summary.p95_ms,
        headings_total as f64 / args.iterations as f64
    );
    let report = json!({
        "scenario": "parse",
        "iterations": args.iterations,
        "docs_loaded": docs.len(),
        "generated_at_unix": now_unix(),
        "latency": summary.as_json(),
        "headings_avg": round_ms(headings_total as f64 / args.iterations as f64),
    });
    write_benchmark_reports(args, &report, "parse")?;

    Ok(())
}

fn run_search_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("search benchmark iterations must be greater than zero");
    }
    let (vault_root, db_path, _temp) = search_benchmark_vault(args)?;
    let request = VaultSearchRequest {
        vault_root: vault_root.clone(),
        query: Some(args.query_text.trim().to_string()),
        path: None,
        kind: SearchKind::Auto,
        scope: None,
        extensions: Vec::new(),
        include_context: true,
        depth: 2,
        limit: u32::try_from(args.query_limit.clamp(1, 100))?,
        include_content: false,
        include_pii: true,
    };
    if request.query.as_deref().unwrap_or_default().is_empty() {
        bail!("query text must not be empty");
    }

    let connection =
        Connection::open(&db_path).with_context(|| format!("open sqlite {}", db_path.display()))?;
    let service = VaultSearchService;
    let mut samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut candidates_total = 0_u64;
    for _ in 0..args.iterations {
        let start = Instant::now();
        let result = service
            .search(&connection, request.clone())
            .context("vault search sample failed")?;
        samples.push(elapsed_ms(start));
        candidates_total = candidates_total.saturating_add(u64::try_from(result.candidates.len())?);
        std::hint::black_box(result);
    }

    let summary = LatencySummary::from_samples(samples)?;
    println!(
        "search p50_ms={:.3} p95_ms={:.3} candidates_avg={:.1}",
        summary.p50_ms,
        summary.p95_ms,
        candidates_total as f64 / args.iterations as f64
    );

    let report = json!({
        "scenario": "search",
        "iterations": args.iterations,
        "generated_at_unix": now_unix(),
        "vault_root": vault_root,
        "db_path": db_path.display().to_string(),
        "request": {
            "query": request.query,
            "kind": request.kind.label(),
            "limit": request.limit,
            "context": request.include_context,
            "depth": request.depth,
        },
        "latency": summary.as_json(),
        "candidates_avg": round_ms(candidates_total as f64 / args.iterations as f64),
    });
    write_benchmark_reports(args, &report, "search")?;
    Ok(())
}

fn run_resolve_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("resolve benchmark iterations must be greater than zero");
    }

    let candidate_total = usize::try_from(args.bridge_notes.max(1_000))
        .context("convert resolve candidate count to usize")?;
    let mut candidates = Vec::with_capacity(candidate_total + (candidate_total / 5));
    for index in 0..candidate_total {
        candidates.push(format!("notes/note-{index:05}.md"));
    }
    for index in 0..(candidate_total / 5) {
        candidates.push(format!("archive/note-{index:05}.md"));
    }
    let resolution_index = LinkResolutionIndex::new(&candidates);

    let links_per_iteration = 256_u64;
    let mut samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let benchmark_start = Instant::now();

    for iteration in 0..args.iterations {
        let start = Instant::now();
        for offset in 0..links_per_iteration {
            let index = usize::try_from((iteration + offset) % args.bridge_notes.max(1_000))
                .context("convert resolver index to usize")?;
            let target = format!("[[note-{index:05}]]");
            let resolution = resolution_index.resolve(&target, Some("notes/current.md"));
            std::hint::black_box(resolution.resolved_path);
        }
        samples.push(elapsed_ms(start));
    }

    let elapsed_ms_total = elapsed_ms(benchmark_start);
    let summary = LatencySummary::from_samples(samples)?;
    let total_ops = args.iterations.saturating_mul(links_per_iteration);
    let throughput_ops_per_sec = if elapsed_ms_total == 0.0 {
        0.0
    } else {
        (total_ops as f64) / (elapsed_ms_total / 1_000.0)
    };

    println!(
        "resolve p50_ms={:.3} p95_ms={:.3} max_ms={:.3} ops_per_sec={:.1}",
        summary.p50_ms, summary.p95_ms, summary.max_ms, throughput_ops_per_sec
    );

    let report = json!({
        "scenario": "resolve",
        "iterations": args.iterations,
        "links_per_iteration": links_per_iteration,
        "candidates_total": candidates.len(),
        "generated_at_unix": now_unix(),
        "latency": summary.as_json(),
        "throughput_ops_per_sec": round_ms(throughput_ops_per_sec),
    });
    write_benchmark_reports(args, &report, "resolve")?;

    Ok(())
}

fn run_startup_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("startup benchmark iterations must be greater than zero");
    }

    let notes_total = args.bridge_notes.max(1);
    let temp = tempdir().context("create startup benchmark temp directory")?;
    let vault_root = temp.path().join("vault");
    let notes_dir = vault_root.join("notes");
    let db_path = temp.path().join("tao.sqlite");
    fs::create_dir_all(&notes_dir).context("create startup benchmark notes directory")?;
    seed_indexed_bridge_vault(&vault_root, &db_path, notes_total)
        .context("seed startup benchmark vault")?;

    let mut samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    for _ in 0..args.iterations {
        let start = Instant::now();
        let kernel =
            BridgeKernel::open(&vault_root, &db_path).context("open kernel for startup sample")?;
        let _stats = consume_envelope(kernel.vault_stats(), "startup_vault_stats")?;
        let page = consume_envelope(kernel.notes_list(None, 1000), "startup_notes_list")?;
        if let Some(first) = page.items.first() {
            let _context =
                consume_envelope(kernel.note_context(&first.path), "startup_note_context")?;
        }
        samples.push(elapsed_ms(start));
    }

    let summary = LatencySummary::from_samples(samples)?;
    let target_p95_ms = 900.0;
    let budget_failed = summary.p95_ms > target_p95_ms;
    let status = if budget_failed { "fail" } else { "pass" };

    println!(
        "startup p50_ms={:.3} p95_ms={:.3} max_ms={:.3} target_p95_ms={:.1} status={status}",
        summary.p50_ms, summary.p95_ms, summary.max_ms, target_p95_ms
    );

    let report = json!({
        "scenario": "startup",
        "iterations": args.iterations,
        "notes_seeded": notes_total,
        "generated_at_unix": now_unix(),
        "latency": summary.as_json(),
        "budget": {
            "target_p95_ms": target_p95_ms,
        },
        "status": status,
    });
    write_benchmark_reports(args, &report, "startup")?;
    enforce_startup_budget(args, budget_failed, summary.p95_ms, target_p95_ms)?;

    Ok(())
}

fn run_graph_walk_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("graph-walk benchmark iterations must be greater than zero");
    }
    let (vault_root, db_path) = resolve_vault_and_db_paths(args)?;
    let request = GraphWalkRequest {
        path: args.graph_root.clone(),
        depth: args.graph_depth.max(1),
        limit: args.graph_limit.max(1),
        include_unresolved: true,
        include_folders: args.graph_include_folders,
    };

    let mut warm_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut cold_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut warm_steps = 0_u64;
    let mut cold_steps = 0_u64;

    let warm_connection =
        Connection::open(&db_path).with_context(|| format!("open sqlite {}", db_path.display()))?;
    for _ in 0..args.iterations {
        let start = Instant::now();
        let steps = BacklinkGraphService
            .walk(&warm_connection, &request)
            .context("graph walk warm sample failed")?;
        warm_samples.push(elapsed_ms(start));
        warm_steps = warm_steps.saturating_add(u64::try_from(steps.len()).unwrap_or(0));
    }
    for _ in 0..args.iterations {
        let start = Instant::now();
        let cold_connection = Connection::open(&db_path)
            .with_context(|| format!("open sqlite {}", db_path.display()))?;
        let steps = BacklinkGraphService
            .walk(&cold_connection, &request)
            .context("graph walk cold sample failed")?;
        cold_samples.push(elapsed_ms(start));
        cold_steps = cold_steps.saturating_add(u64::try_from(steps.len()).unwrap_or(0));
    }

    let warm = LatencySummary::from_samples(warm_samples)?;
    let cold = LatencySummary::from_samples(cold_samples)?;
    let improvement_pct = if cold.p50_ms == 0.0 {
        0.0
    } else {
        ((cold.p50_ms - warm.p50_ms) / cold.p50_ms) * 100.0
    };

    println!(
        "graph-walk warm_p50_ms={:.3} cold_p50_ms={:.3} warm_steps_avg={:.1} cold_steps_avg={:.1}",
        warm.p50_ms,
        cold.p50_ms,
        warm_steps as f64 / args.iterations as f64,
        cold_steps as f64 / args.iterations as f64
    );

    let report = json!({
        "scenario": "graph_walk",
        "iterations": args.iterations,
        "generated_at_unix": now_unix(),
        "vault_root": vault_root,
        "db_path": db_path.display().to_string(),
        "request": {
            "path": request.path,
            "depth": request.depth,
            "limit": request.limit,
            "include_unresolved": request.include_unresolved,
            "include_folders": request.include_folders,
        },
        "latency": {
            "warm": warm.as_json(),
            "cold": cold.as_json(),
        },
        "steps": {
            "warm_total": warm_steps,
            "cold_total": cold_steps,
            "warm_avg": round_ms(warm_steps as f64 / args.iterations as f64),
            "cold_avg": round_ms(cold_steps as f64 / args.iterations as f64),
        },
        "improvement": {
            "p50_vs_cold_pct": round_ms(improvement_pct),
        },
    });
    write_benchmark_reports(args, &report, "graph_walk")?;
    Ok(())
}

fn run_unified_query_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("unified-query benchmark iterations must be greater than zero");
    }
    let (vault_root, db_path) = resolve_vault_and_db_paths(args)?;
    let request = SearchQueryRequest {
        query: args.query_text.trim().to_string(),
        limit: args.query_limit.clamp(1, 1_000),
        offset: 0,
    };
    if request.query.is_empty() {
        bail!("query text must not be empty");
    }

    let mut warm_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut cold_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut warm_rows = 0_u64;
    let mut cold_rows = 0_u64;

    let warm_connection =
        Connection::open(&db_path).with_context(|| format!("open sqlite {}", db_path.display()))?;
    let _ = SearchQueryService
        .query(&vault_root, &warm_connection, request.clone())
        .context("prime warm query cache")?;
    for _ in 0..args.iterations {
        let start = Instant::now();
        let page = SearchQueryService
            .query(&vault_root, &warm_connection, request.clone())
            .context("warm unified query sample failed")?;
        warm_samples.push(elapsed_ms(start));
        warm_rows = warm_rows.saturating_add(u64::try_from(page.items.len()).unwrap_or(0));
    }
    for _ in 0..args.iterations {
        let start = Instant::now();
        let cold_connection = Connection::open(&db_path)
            .with_context(|| format!("open sqlite {}", db_path.display()))?;
        let page = SearchQueryService
            .query(&vault_root, &cold_connection, request.clone())
            .context("cold unified query sample failed")?;
        cold_samples.push(elapsed_ms(start));
        cold_rows = cold_rows.saturating_add(u64::try_from(page.items.len()).unwrap_or(0));
    }

    let warm = LatencySummary::from_samples(warm_samples)?;
    let cold = LatencySummary::from_samples(cold_samples)?;
    let improvement_pct = if cold.p50_ms == 0.0 {
        0.0
    } else {
        ((cold.p50_ms - warm.p50_ms) / cold.p50_ms) * 100.0
    };

    println!(
        "unified-query warm_p50_ms={:.3} cold_p50_ms={:.3} warm_rows_avg={:.1} cold_rows_avg={:.1}",
        warm.p50_ms,
        cold.p50_ms,
        warm_rows as f64 / args.iterations as f64,
        cold_rows as f64 / args.iterations as f64
    );

    let report = json!({
        "scenario": "unified_query",
        "iterations": args.iterations,
        "generated_at_unix": now_unix(),
        "vault_root": vault_root,
        "db_path": db_path.display().to_string(),
        "request": {
            "query": request.query,
            "limit": request.limit,
            "offset": request.offset,
        },
        "latency": {
            "warm": warm.as_json(),
            "cold": cold.as_json(),
        },
        "rows": {
            "warm_total": warm_rows,
            "cold_total": cold_rows,
            "warm_avg": round_ms(warm_rows as f64 / args.iterations as f64),
            "cold_avg": round_ms(cold_rows as f64 / args.iterations as f64),
        },
        "improvement": {
            "p50_vs_cold_pct": round_ms(improvement_pct),
        },
    });
    write_benchmark_reports(args, &report, "unified_query")?;
    Ok(())
}

fn run_bridge_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("bridge benchmark iterations must be greater than zero");
    }

    let notes_total = args.bridge_notes.max(1);
    let temp = tempdir().context("create benchmark temp directory")?;
    let vault_root = temp.path().join("vault");
    let notes_dir = vault_root.join("notes");
    let db_path = temp.path().join("tao.sqlite");
    fs::create_dir_all(&notes_dir).context("create benchmark notes directory")?;
    seed_indexed_bridge_vault(&vault_root, &db_path, notes_total)
        .context("seed bridge benchmark vault")?;

    let kernel = BridgeKernel::open(&vault_root, &db_path).context("open bridge kernel")?;

    let mut note_get_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut notes_list_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut note_context_samples =
        Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut events_poll_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut event_cursor = 0_u64;

    for iteration in 0..args.iterations {
        let idx = iteration % notes_total;
        let path = format!("notes/note-{idx:05}.md");

        let note_get_start = Instant::now();
        consume_envelope(kernel.note_get(&path), "note_get")?;
        note_get_samples.push(elapsed_ms(note_get_start));

        let notes_list_start = Instant::now();
        consume_envelope(kernel.notes_list(None, 64), "notes_list")?;
        notes_list_samples.push(elapsed_ms(notes_list_start));

        let note_context_start = Instant::now();
        consume_envelope(kernel.note_context(&path), "note_context")?;
        note_context_samples.push(elapsed_ms(note_context_start));

        let events_poll_start = Instant::now();
        let batch = consume_envelope(kernel.events_poll(event_cursor, 256), "events_poll")?;
        event_cursor = batch.next_cursor;
        events_poll_samples.push(elapsed_ms(events_poll_start));
    }

    let note_get = LatencySummary::from_samples(note_get_samples)?;
    let notes_list = LatencySummary::from_samples(notes_list_samples)?;
    let note_context = LatencySummary::from_samples(note_context_samples)?;
    let events_poll = LatencySummary::from_samples(events_poll_samples)?;

    println!(
        "bridge metric=note_get p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        note_get.p50_ms, note_get.p95_ms, note_get.max_ms
    );
    println!(
        "bridge metric=notes_list p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        notes_list.p50_ms, notes_list.p95_ms, notes_list.max_ms
    );
    println!(
        "bridge metric=note_context p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        note_context.p50_ms, note_context.p95_ms, note_context.max_ms
    );
    println!(
        "bridge metric=events_poll p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        events_poll.p50_ms, events_poll.p95_ms, events_poll.max_ms
    );

    let mut violations = Vec::new();
    check_budget(
        "note_get",
        note_get,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );
    check_budget(
        "notes_list",
        notes_list,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );
    check_budget(
        "note_context",
        note_context,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );
    check_budget(
        "events_poll",
        events_poll,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );

    let report = json!({
        "scenario": "bridge",
        "iterations": args.iterations,
        "notes_seeded": notes_total,
        "generated_at_unix": now_unix(),
        "budgets": {
            "max_p50_ms": args.max_p50_ms,
            "max_p95_ms": args.max_p95_ms,
        },
        "metrics": {
            "note_get": note_get.as_json(),
            "notes_list": notes_list.as_json(),
            "note_context": note_context.as_json(),
            "events_poll": events_poll.as_json(),
        },
        "violations": violations,
        "status": if violations.is_empty() { "pass" } else { "fail" },
    });

    write_benchmark_reports(args, &report, "bridge")?;

    if args.enforce_budgets && !violations.is_empty() {
        bail!(
            "bridge benchmark exceeded budgets: {}",
            violations.join("; ")
        );
    }

    Ok(())
}

fn parse_benchmark_vault(args: &Args) -> Result<(PathBuf, Option<TempDir>)> {
    if let Some(vault_root) = &args.vault_root {
        if !vault_root.is_dir() {
            bail!(
                "vault root does not exist or is not a directory: {}",
                vault_root.display()
            );
        }
        return Ok((vault_root.clone(), None));
    }

    let temp = tempdir().context("create parse benchmark temp directory")?;
    let vault_root = temp.path().join("vault");
    let notes_dir = vault_root.join("notes");
    fs::create_dir_all(&notes_dir).context("create parse benchmark notes directory")?;
    for idx in 0..args.bridge_notes.max(128) {
        let path = notes_dir.join(format!("parse-{idx:05}.md"));
        let content = format!(
            "---\nkind: benchmark\nindex: {idx}\n---\n# Parse {idx}\n\nProject benchmark note with [[parse-{next:05}]].\n\n- [ ] Follow up on project {idx}\n",
            next = (idx + 1) % args.bridge_notes.max(128),
        );
        fs::write(&path, content).with_context(|| format!("write {}", path.display()))?;
    }
    Ok((vault_root, Some(temp)))
}

fn search_benchmark_vault(args: &Args) -> Result<(PathBuf, PathBuf, Option<TempDir>)> {
    if args.vault_root.is_some() || args.db_path.is_some() {
        let (vault_root, db_path) = resolve_vault_and_db_paths(args)?;
        return Ok((vault_root, db_path, None));
    }

    let temp = tempdir().context("create search benchmark temp directory")?;
    let vault_root = temp.path().join("vault");
    let db_path = temp.path().join("tao.sqlite");
    seed_indexed_bridge_vault(&vault_root, &db_path, args.bridge_notes.max(128))
        .context("seed search benchmark vault")?;
    Ok((vault_root, db_path, Some(temp)))
}

fn collect_markdown_payloads(vault_root: &Path, limit: usize) -> Result<Vec<(String, String)>> {
    let mut paths = Vec::<PathBuf>::new();
    collect_markdown_paths(vault_root, &mut paths)?;
    paths.sort();
    paths.truncate(limit.max(1));

    let mut payloads = Vec::with_capacity(paths.len());
    for path in paths {
        let normalized = path
            .strip_prefix(vault_root)
            .with_context(|| format!("strip vault prefix from {}", path.display()))?
            .to_string_lossy()
            .replace('\\', "/");
        let raw = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        payloads.push((normalized, raw));
    }
    Ok(payloads)
}

fn collect_markdown_paths(current: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(current).with_context(|| format!("read dir {}", current.display()))? {
        let entry = entry.with_context(|| format!("read dir entry {}", current.display()))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name == ".git" || name == ".obsidian" || name == ".tao" {
            continue;
        }
        let metadata = entry
            .metadata()
            .with_context(|| format!("metadata {}", path.display()))?;
        if metadata.is_dir() {
            collect_markdown_paths(&path, paths)?;
        } else if metadata.is_file()
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
        {
            paths.push(path);
        }
    }
    Ok(())
}

fn seed_indexed_bridge_vault(vault_root: &Path, db_path: &Path, notes_total: u64) -> Result<()> {
    let notes_dir = vault_root.join("notes");
    fs::create_dir_all(&notes_dir).context("create bridge benchmark notes directory")?;

    for idx in 0..notes_total {
        let next = (idx + 1) % notes_total;
        let path = notes_dir.join(format!("note-{idx:05}.md"));
        let content = format!(
            "# Note {idx}\n\nProject seed note with [[note-{next:05}]].\n\n- [ ] Follow project benchmark task {idx}\n"
        );
        fs::write(&path, content).with_context(|| format!("write seed note {}", path.display()))?;
    }

    let mut kernel =
        BridgeKernel::open(vault_root, db_path).context("open bridge kernel for seed indexing")?;
    kernel
        .ensure_indexed()
        .context("index seeded bridge benchmark vault")?;
    Ok(())
}

fn consume_envelope<T>(envelope: BridgeEnvelope<T>, operation: &str) -> Result<T> {
    if envelope.ok {
        return envelope
            .value
            .with_context(|| format!("{operation} returned ok envelope without value payload"));
    }

    let error = envelope
        .error
        .with_context(|| format!("{operation} returned failed envelope without error payload"))?;
    bail!("{operation} failed: {} ({})", error.message, error.code);
}

fn check_budget(
    metric: &str,
    summary: LatencySummary,
    max_p50_ms: f64,
    max_p95_ms: f64,
    violations: &mut Vec<String>,
) {
    if summary.p50_ms > max_p50_ms {
        violations.push(format!(
            "{metric}.p50_ms {:.3} exceeded {:.3}",
            summary.p50_ms, max_p50_ms
        ));
    }
    if summary.p95_ms > max_p95_ms {
        violations.push(format!(
            "{metric}.p95_ms {:.3} exceeded {:.3}",
            summary.p95_ms, max_p95_ms
        ));
    }
}

fn percentile(sorted_samples: &[f64], percentile: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }

    let max_index = sorted_samples.len().saturating_sub(1);
    let rank = ((percentile / 100.0) * (max_index as f64)).round();
    let index = usize::try_from(rank as u64)
        .unwrap_or(max_index)
        .min(max_index);
    sorted_samples[index]
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn round_ms(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn resolve_vault_and_db_paths(args: &Args) -> Result<(PathBuf, PathBuf)> {
    let vault_root = args
        .vault_root
        .clone()
        .context("scenario requires --vault-root path to indexed fixture vault")?;
    let db_path = args
        .db_path
        .clone()
        .context("scenario requires --db-path path to indexed sqlite database")?;
    if !vault_root.is_dir() {
        bail!(
            "vault root does not exist or is not a directory: {}",
            vault_root.display()
        );
    }
    if !db_path.is_file() {
        bail!(
            "db path does not exist or is not a file: {}",
            db_path.display()
        );
    }
    Ok((vault_root, db_path))
}

fn write_benchmark_reports(args: &Args, report: &JsonValue, scenario: &str) -> Result<()> {
    validate_report_output_paths(args)?;
    if let Some(path) = &args.json_out {
        write_json_report(path, report)?;
        println!("{scenario} report written to {}", path.display());
    }
    if let Some(path) = &args.markdown_out {
        write_markdown_summary(path, report)?;
        println!("{scenario} markdown summary written to {}", path.display());
    }
    Ok(())
}

fn validate_report_output_paths(args: &Args) -> Result<()> {
    if let (Some(json_path), Some(markdown_path)) = (&args.json_out, &args.markdown_out)
        && report_output_identity(json_path)? == report_output_identity(markdown_path)?
    {
        bail!(
            "--json-out and --markdown-out must be different paths: {}",
            json_path.display()
        );
    }
    Ok(())
}

fn report_output_identity(path: &Path) -> Result<PathBuf> {
    if let Ok(canonical) = fs::canonicalize(path) {
        return Ok(canonical);
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current directory for report output path")?
            .join(path)
    };
    if let (Some(parent), Some(file_name)) = (absolute.parent(), absolute.file_name())
        && let Ok(canonical_parent) = fs::canonicalize(parent)
    {
        return Ok(canonical_parent.join(file_name));
    }
    Ok(normalize_lexical_path(&absolute))
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn write_markdown_summary(path: &Path, report: &JsonValue) -> Result<()> {
    let scenario = report
        .get("scenario")
        .and_then(JsonValue::as_str)
        .unwrap_or("unknown");
    let iterations = report
        .get("iterations")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);

    let mut markdown = String::new();
    markdown.push_str("# Tao Bench Summary\n\n");
    markdown.push_str(&format!("- scenario: `{scenario}`\n"));
    markdown.push_str(&format!("- iterations: `{iterations}`\n"));
    markdown.push_str(&format!("- generated_at_unix: `{}`\n\n", now_unix()));
    if let Some(metrics) = report.get("metrics").and_then(JsonValue::as_object) {
        markdown.push_str("| metric | p50_ms | p95_ms | max_ms |\n");
        markdown.push_str("| --- | ---: | ---: | ---: |\n");
        for (metric, values) in metrics {
            markdown.push_str(&format!(
                "| {metric} | {:.3} | {:.3} | {:.3} |\n",
                json_number(values, "p50_ms"),
                json_number(values, "p95_ms"),
                json_number(values, "max_ms")
            ));
        }
        markdown.push('\n');
    } else if let Some(latency) = report.get("latency") {
        markdown.push_str("| mode | p50_ms | p95_ms | max_ms |\n");
        markdown.push_str("| --- | ---: | ---: | ---: |\n");
        if latency.get("warm").is_some() || latency.get("cold").is_some() {
            write_latency_row(&mut markdown, "warm", latency.get("warm"));
            write_latency_row(&mut markdown, "cold", latency.get("cold"));
        } else {
            write_latency_row(&mut markdown, "sample", Some(latency));
        }
        markdown.push('\n');
    }
    if let Some(improvement_pct) = report
        .pointer("/improvement/p50_vs_cold_pct")
        .and_then(JsonValue::as_f64)
    {
        markdown.push_str(&format!(
            "- warm_vs_cold_p50_improvement_pct: `{improvement_pct:.3}`\n"
        ));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create markdown report dir {}", parent.display()))?;
    }
    fs::write(path, markdown)
        .with_context(|| format!("write markdown benchmark report {}", path.display()))?;
    Ok(())
}

fn write_json_report(path: &Path, report: &JsonValue) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create report dir {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(report).context("serialize benchmark report json")?;
    fs::write(path, bytes).with_context(|| format!("write benchmark report {}", path.display()))?;
    Ok(())
}

fn write_latency_row(markdown: &mut String, mode: &str, values: Option<&JsonValue>) {
    markdown.push_str(&format!(
        "| {mode} | {:.3} | {:.3} | {:.3} |\n",
        values.map_or(0.0, |value| json_number(value, "p50_ms")),
        values.map_or(0.0, |value| json_number(value, "p95_ms")),
        values.map_or(0.0, |value| json_number(value, "max_ms"))
    ));
}

fn json_number(value: &JsonValue, key: &str) -> f64 {
    value.get(key).and_then(JsonValue::as_f64).unwrap_or(0.0)
}

fn enforce_startup_budget(
    args: &Args,
    budget_failed: bool,
    p95_ms: f64,
    target_p95_ms: f64,
) -> Result<()> {
    if args.enforce_budgets && budget_failed {
        bail!(
            "startup benchmark exceeded p95 budget: p95_ms={p95_ms:.3} target_p95_ms={target_p95_ms:.1}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report_args(markdown_out: Option<PathBuf>) -> Args {
        Args {
            scenario: Scenario::Parse,
            iterations: 1,
            bridge_notes: 1,
            max_p50_ms: 50.0,
            max_p95_ms: 120.0,
            enforce_budgets: false,
            json_out: None,
            markdown_out,
            vault_root: None,
            db_path: None,
            graph_root: "notes/projects/project-1.md".to_string(),
            graph_depth: 2,
            graph_limit: 200,
            graph_include_folders: false,
            query_text: "project".to_string(),
            query_limit: 100,
        }
    }

    #[test]
    fn report_writer_honors_markdown_out_for_simple_latency() {
        let temp = tempdir().expect("tempdir");
        let markdown_out = temp.path().join("parse.md");
        let report = json!({
            "scenario": "parse",
            "iterations": 1,
            "generated_at_unix": 1,
            "latency": {
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "max_ms": 3.0,
            },
        });

        write_benchmark_reports(&report_args(Some(markdown_out.clone())), &report, "parse")
            .expect("write markdown report");

        let markdown = std::fs::read_to_string(markdown_out).expect("read markdown report");
        assert!(markdown.contains("- scenario: `parse`"));
        assert!(markdown.contains("| sample | 1.000 | 2.000 | 3.000 |"));
    }

    #[test]
    fn report_writer_honors_markdown_out_for_bridge_metrics() {
        let temp = tempdir().expect("tempdir");
        let markdown_out = temp.path().join("bridge.md");
        let report = json!({
            "scenario": "bridge",
            "iterations": 1,
            "generated_at_unix": 1,
            "metrics": {
                "note_get": {
                    "p50_ms": 1.0,
                    "p95_ms": 2.0,
                    "max_ms": 3.0,
                },
            },
        });

        write_benchmark_reports(&report_args(Some(markdown_out.clone())), &report, "bridge")
            .expect("write markdown report");

        let markdown = std::fs::read_to_string(markdown_out).expect("read markdown report");
        assert!(markdown.contains("- scenario: `bridge`"));
        assert!(markdown.contains("| note_get | 1.000 | 2.000 | 3.000 |"));
    }

    #[test]
    fn report_writer_does_not_infer_markdown_out_from_json_out() {
        let temp = tempdir().expect("tempdir");
        let json_out = temp.path().join("bench.md");
        let mut args = report_args(None);
        args.json_out = Some(json_out.clone());
        let report = json!({
            "scenario": "parse",
            "iterations": 1,
            "generated_at_unix": 1,
            "latency": {
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "max_ms": 3.0,
            },
        });

        write_benchmark_reports(&args, &report, "parse").expect("write json report");

        let written = std::fs::read_to_string(json_out).expect("read json report");
        let parsed: JsonValue = serde_json::from_str(&written).expect("json report remains json");
        assert_eq!(parsed["scenario"], "parse");
    }

    #[test]
    fn report_writer_rejects_identical_json_and_markdown_paths_before_write() {
        let temp = tempdir().expect("tempdir");
        let report_path = temp.path().join("bench.json");
        std::fs::write(&report_path, "sentinel").expect("seed report file");
        let mut args = report_args(Some(report_path.clone()));
        args.json_out = Some(report_path.clone());
        let report = json!({
            "scenario": "parse",
            "iterations": 1,
            "generated_at_unix": 1,
            "latency": {
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "max_ms": 3.0,
            },
        });

        let error = write_benchmark_reports(&args, &report, "parse")
            .expect_err("identical report paths should be rejected");

        assert!(
            error
                .to_string()
                .contains("--json-out and --markdown-out must be different paths")
        );
        let written = std::fs::read_to_string(report_path).expect("read seeded report file");
        assert_eq!(written, "sentinel");
    }

    #[test]
    fn report_writer_rejects_normalized_json_and_markdown_path_collision() {
        let temp = tempdir().expect("tempdir");
        let reports_dir = temp.path().join("reports");
        std::fs::create_dir_all(&reports_dir).expect("create reports dir");
        let report_path = reports_dir.join("bench.json");
        std::fs::write(&report_path, "sentinel").expect("seed report file");
        let mut args = report_args(Some(reports_dir.join(".").join("bench.json")));
        args.json_out = Some(report_path.clone());
        let report = json!({
            "scenario": "parse",
            "iterations": 1,
            "generated_at_unix": 1,
            "latency": {
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "max_ms": 3.0,
            },
        });

        let error = write_benchmark_reports(&args, &report, "parse")
            .expect_err("normalized report path collision should be rejected");

        assert!(
            error
                .to_string()
                .contains("--json-out and --markdown-out must be different paths")
        );
        let written = std::fs::read_to_string(report_path).expect("read seeded report file");
        assert_eq!(written, "sentinel");
    }

    #[test]
    fn startup_budget_enforcement_rejects_failed_status() {
        let temp = tempdir().expect("tempdir");
        let mut args = report_args(Some(temp.path().join("startup.md")));
        args.scenario = Scenario::Startup;
        args.enforce_budgets = true;

        let error = enforce_startup_budget(&args, true, 901.0, 900.0)
            .expect_err("failed startup budget should be enforced");

        assert!(
            error
                .to_string()
                .contains("startup benchmark exceeded p95 budget")
        );
    }
}
