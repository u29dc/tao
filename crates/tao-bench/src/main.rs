use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use serde_json::{Value as JsonValue, json};
use tao_sdk_bridge::{BridgeEnvelope, BridgeKernel, TaoBridgeRuntime};
use tao_sdk_links::resolve_target;
use tempfile::tempdir;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Scenario {
    Parse,
    Resolve,
    Search,
    Bridge,
    Ffi,
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
        Scenario::Ffi => run_ffi_benchmark(&args),
        Scenario::Resolve => run_resolve_benchmark(&args),
        Scenario::Startup => run_startup_benchmark(&args),
        Scenario::Parse | Scenario::Search => run_cpu_smoke_benchmark(&args),
    }
}

fn run_cpu_smoke_benchmark(args: &Args) -> Result<()> {
    let start = Instant::now();
    for i in 0..args.iterations {
        std::hint::black_box(i.wrapping_mul(31).wrapping_add(7));
    }
    let elapsed_ms = elapsed_ms(start);

    println!(
        "scenario={:?} iterations={} elapsed_ms={:.3}",
        args.scenario, args.iterations, elapsed_ms
    );

    if let Some(path) = &args.json_out {
        let report = json!({
            "scenario": format!("{:?}", args.scenario).to_lowercase(),
            "iterations": args.iterations,
            "elapsed_ms": round_ms(elapsed_ms),
            "generated_at_unix": now_unix(),
        });
        write_json_report(path, &report)?;
    }

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

    let links_per_iteration = 256_u64;
    let mut samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let benchmark_start = Instant::now();

    for iteration in 0..args.iterations {
        let start = Instant::now();
        for offset in 0..links_per_iteration {
            let index = usize::try_from((iteration + offset) % args.bridge_notes.max(1_000))
                .context("convert resolver index to usize")?;
            let target = format!("[[note-{index:05}]]");
            let resolution = resolve_target(&target, Some("notes/current.md"), &candidates);
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
    if let Some(path) = &args.json_out {
        write_json_report(path, &report)?;
        println!("resolve report written to {}", path.display());
    }

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

    let mut seed_kernel = BridgeKernel::open(&vault_root, &db_path)
        .context("open startup benchmark bridge kernel")?;
    for idx in 0..notes_total {
        let path = format!("notes/note-{idx:05}.md");
        let content = format!("# Note {idx}\nstartup seed");
        consume_envelope(
            seed_kernel.note_put_with_policy(&path, &content, true),
            "startup_seed_note_put",
        )?;
    }

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
    let status = if summary.p95_ms <= target_p95_ms {
        "pass"
    } else {
        "fail"
    };

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
    if let Some(path) = &args.json_out {
        write_json_report(path, &report)?;
        println!("startup report written to {}", path.display());
    }

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

    let mut kernel = BridgeKernel::open(&vault_root, &db_path).context("open bridge kernel")?;

    for idx in 0..notes_total {
        let path = format!("notes/note-{idx:05}.md");
        let content = format!("# Note {idx}\nseed");
        consume_envelope(
            kernel.note_put_with_policy(&path, &content, true),
            "seed_note_put",
        )?;
    }

    let mut note_get_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut notes_list_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut note_put_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
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

        let content = format!("# Note {idx}\niteration {iteration}");
        let note_put_start = Instant::now();
        consume_envelope(
            kernel.note_put_with_policy(&path, &content, true),
            "note_put",
        )?;
        note_put_samples.push(elapsed_ms(note_put_start));

        let events_poll_start = Instant::now();
        let batch = consume_envelope(kernel.events_poll(event_cursor, 256), "events_poll")?;
        event_cursor = batch.next_cursor;
        events_poll_samples.push(elapsed_ms(events_poll_start));
    }

    let note_get = LatencySummary::from_samples(note_get_samples)?;
    let notes_list = LatencySummary::from_samples(notes_list_samples)?;
    let note_put = LatencySummary::from_samples(note_put_samples)?;
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
        "bridge metric=note_put p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        note_put.p50_ms, note_put.p95_ms, note_put.max_ms
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
        "note_put",
        note_put,
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
            "note_put": note_put.as_json(),
            "events_poll": events_poll.as_json(),
        },
        "violations": violations,
        "status": if violations.is_empty() { "pass" } else { "fail" },
    });

    if let Some(path) = &args.json_out {
        write_json_report(path, &report)?;
        println!("bridge report written to {}", path.display());
    }

    if args.enforce_budgets && !violations.is_empty() {
        bail!(
            "bridge benchmark exceeded budgets: {}",
            violations.join("; ")
        );
    }

    Ok(())
}

fn run_ffi_benchmark(args: &Args) -> Result<()> {
    if args.iterations == 0 {
        bail!("ffi benchmark iterations must be greater than zero");
    }

    let notes_total = args.bridge_notes.max(1);
    let temp = tempdir().context("create ffi benchmark temp directory")?;
    let vault_root = temp.path().join("vault");
    fs::create_dir_all(vault_root.join("notes")).context("create ffi benchmark notes directory")?;

    let runtime = TaoBridgeRuntime::new(vault_root.to_string_lossy().to_string(), None)
        .map_err(|source| anyhow::anyhow!("create ffi runtime failed: {source}"))?;

    for idx in 0..notes_total {
        let path = format!("notes/note-{idx:05}.md");
        let content = format!("# Note {idx}\nseed");
        let _: JsonValue = consume_json_envelope(
            runtime
                .note_put_json(path, content, Some(true))
                .map_err(|source| anyhow::anyhow!("ffi note_put_json failed: {source}"))?,
            "ffi_note_put_seed",
        )?;
    }

    let mut note_open_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut tree_window_samples = Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));
    let mut startup_bundle_samples =
        Vec::with_capacity(usize::try_from(args.iterations).unwrap_or(0));

    for iteration in 0..args.iterations {
        let idx = iteration % notes_total;
        let path = format!("notes/note-{idx:05}.md");

        let note_open_start = Instant::now();
        let _note_context: JsonValue = consume_json_envelope(
            runtime
                .note_context_json(path)
                .map_err(|source| anyhow::anyhow!("ffi note_context_json failed: {source}"))?,
            "ffi_note_context",
        )?;
        note_open_samples.push(elapsed_ms(note_open_start));

        let tree_window_start = Instant::now();
        let _window: JsonValue = consume_json_envelope(
            runtime
                .notes_window_json(None, Some(512))
                .map_err(|source| anyhow::anyhow!("ffi notes_window_json failed: {source}"))?,
            "ffi_notes_window",
        )?;
        tree_window_samples.push(elapsed_ms(tree_window_start));

        let startup_bundle_start = Instant::now();
        let _: JsonValue = serde_json::from_str(
            &runtime
                .startup_bundle_json(Some(512))
                .map_err(|source| anyhow::anyhow!("ffi startup_bundle_json failed: {source}"))?,
        )
        .context("decode ffi startup bundle json")?;
        startup_bundle_samples.push(elapsed_ms(startup_bundle_start));
    }

    let note_open = LatencySummary::from_samples(note_open_samples)?;
    let tree_window = LatencySummary::from_samples(tree_window_samples)?;
    let startup_bundle = LatencySummary::from_samples(startup_bundle_samples)?;

    println!(
        "ffi metric=note_open p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        note_open.p50_ms, note_open.p95_ms, note_open.max_ms
    );
    println!(
        "ffi metric=tree_window p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        tree_window.p50_ms, tree_window.p95_ms, tree_window.max_ms
    );
    println!(
        "ffi metric=startup_bundle p50_ms={:.3} p95_ms={:.3} max_ms={:.3}",
        startup_bundle.p50_ms, startup_bundle.p95_ms, startup_bundle.max_ms
    );

    let mut violations = Vec::new();
    check_budget(
        "note_open",
        note_open,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );
    check_budget(
        "tree_window",
        tree_window,
        args.max_p50_ms,
        args.max_p95_ms,
        &mut violations,
    );
    check_budget(
        "startup_bundle",
        startup_bundle,
        args.max_p50_ms * 2.0,
        args.max_p95_ms * 2.0,
        &mut violations,
    );

    let report = json!({
        "scenario": "ffi",
        "iterations": args.iterations,
        "notes_seeded": notes_total,
        "generated_at_unix": now_unix(),
        "budgets": {
            "max_p50_ms": args.max_p50_ms,
            "max_p95_ms": args.max_p95_ms,
            "startup_multiplier": 2.0,
        },
        "metrics": {
            "note_open": note_open.as_json(),
            "tree_window": tree_window.as_json(),
            "startup_bundle": startup_bundle.as_json(),
        },
        "violations": violations,
        "status": if violations.is_empty() { "pass" } else { "fail" },
    });

    if let Some(path) = &args.json_out {
        write_json_report(path, &report)?;
        println!("ffi report written to {}", path.display());
    }

    if args.enforce_budgets && !violations.is_empty() {
        bail!("ffi benchmark exceeded budgets: {}", violations.join("; "));
    }

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

fn consume_json_envelope<T>(payload: String, operation: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let envelope: BridgeEnvelope<T> = serde_json::from_str(&payload)
        .with_context(|| format!("{operation} decode envelope json"))?;
    consume_envelope(envelope, operation)
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

fn write_json_report(path: &Path, report: &JsonValue) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create report dir {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(report).context("serialize benchmark report json")?;
    fs::write(path, bytes).with_context(|| format!("write benchmark report {}", path.display()))?;
    Ok(())
}
