#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: scripts/bench.sh [--suite SUITE] [--profile PROFILE] [--seed N] [--runs N] [--warmup N] [--output DIR] [--skip-generate]

Unified benchmark driver for Tao SDK/bridge/core/CLI workloads.

Suites:
  all      Run sdk + full read-only cli matrix (default)
  sdk      Run parse/resolve/search/bridge/ffi/startup/graph-walk/unified-query + baseline query/graph budgets
  cli      Run full read-only CLI command matrix
  daemon   Run one-shot vs daemon warm-runtime comparison
  graph-walk Run tao-bench graph-walk scenario
  unified-query Run tao-bench unified-query scenario
  bridge   Run bridge scenario only
  ffi      Run ffi scenario only
  startup  Run startup scenario only
  parse    Run parse scenario only
  resolve  Run resolve scenario only
  search   Run search scenario only

Options:
  --profile PROFILE   Fixture profile for CLI workloads: 1k|5k|10k|25k (default: 10k)
  --seed N            Fixture seed (default: 42)
  --runs N            Hyperfine runs per command (default: 25)
  --warmup N          Hyperfine warmup runs per command (default: 5)
  --output DIR        Benchmark output root (default: .benchmarks/reports)
  --skip-generate     Reuse existing fixture and skip generation/validation
  -h, --help          Show this help
USAGE
}

SUITE="all"
FIXTURE_PROFILE="10k"
SEED="42"
RUNS="25"
WARMUP="5"
OUTPUT_ROOT=".benchmarks/reports"
SKIP_GENERATE=0
FIXTURE_ROOT="vault/generated"
CLI_BUDGET_MS="10"
DAEMON_MIN_IMPROVEMENT_PCT="40"
STREAM_MIN_IMPROVEMENT_PCT="15"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --suite)
      SUITE="${2:-}"
      shift 2
      ;;
    --profile)
      FIXTURE_PROFILE="${2:-}"
      shift 2
      ;;
    --seed)
      SEED="${2:-}"
      shift 2
      ;;
    --runs)
      RUNS="${2:-}"
      shift 2
      ;;
    --warmup)
      WARMUP="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --skip-generate)
      SKIP_GENERATE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$RUNS" =~ ^[0-9]+$ ]]; then
  echo "--runs must be an integer" >&2
  exit 1
fi
if ! [[ "$WARMUP" =~ ^[0-9]+$ ]]; then
  echo "--warmup must be an integer" >&2
  exit 1
fi
if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
  echo "--seed must be an integer" >&2
  exit 1
fi
case "$FIXTURE_PROFILE" in
  1k|5k|10k|25k)
    ;;
  *)
    echo "--profile must be one of: 1k|5k|10k|25k" >&2
    exit 1
    ;;
esac
case "$SUITE" in
  all|sdk|cli|daemon|graph-walk|unified-query|bridge|ffi|startup|parse|resolve|search|core)
    ;;
  *)
    echo "--suite must be one of: all|sdk|cli|daemon|graph-walk|unified-query|bridge|ffi|startup|parse|resolve|search|core" >&2
    exit 1
    ;;
esac

if [[ "$SUITE" == "core" ]]; then
  SUITE="sdk"
fi

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_DIR="${OUTPUT_ROOT}/${RUN_STAMP}"
CLI_MATRIX_REPORT_DIR="${REPORT_DIR}/cli-readonly"
CLI_MATRIX_SUMMARY_PATH="${CLI_MATRIX_REPORT_DIR}/summary.json"
BENCH_BIN="target/release/tao-bench"
CLI_BIN="target/release/tao"
FIXTURE_VAULT="${FIXTURE_ROOT}/vault-${FIXTURE_PROFILE}"
DB_PATH=""
SAMPLE_NOTE="notes/projects/project-1.md"
SAMPLE_BASE="views/projects.base"
SAMPLE_VIEW="Projects"

PARSE_REPORT="${REPORT_DIR}/parse-bench.json"
RESOLVE_REPORT="${REPORT_DIR}/resolve-bench.json"
SEARCH_REPORT="${REPORT_DIR}/search-bench.json"
BRIDGE_REPORT="${REPORT_DIR}/bridge-call-budgets.json"
FFI_REPORT="${REPORT_DIR}/ffi-call-budgets.json"
STARTUP_REPORT="${REPORT_DIR}/startup-budgets.json"
GRAPH_WALK_REPORT="${REPORT_DIR}/graph-walk-bench.json"
UNIFIED_QUERY_REPORT="${REPORT_DIR}/unified-query-bench.json"
HYPERFINE_QUERY_REPORT="${REPORT_DIR}/query-docs-hyperfine.json"
HYPERFINE_GRAPH_REPORT="${REPORT_DIR}/graph-unresolved-hyperfine.json"
DAEMON_REPORT="${REPORT_DIR}/daemon-query-docs-hyperfine.json"
STREAM_COMPARE_REPORT="${REPORT_DIR}/query-docs-stream-vs-standard-hyperfine.json"
STREAM_COMPARE_SUMMARY="${REPORT_DIR}/query-docs-stream-vs-standard.summary.json"
DAEMON_SOCKET=""
DAEMON_RUNNING=0

mkdir -p "${REPORT_DIR}" "${CLI_MATRIX_REPORT_DIR}"
ln -sfn "${RUN_STAMP}" "${OUTPUT_ROOT}/latest"

cleanup_daemon() {
  if [[ "${DAEMON_RUNNING}" -eq 1 ]]; then
    "${CLI_BIN}" --json vault daemon stop --socket "${DAEMON_SOCKET}" >/dev/null 2>&1 || true
    DAEMON_RUNNING=0
  fi
}

trap cleanup_daemon EXIT

build_bins_if_needed() {
  echo "Building release binaries (tao-cli + tao-bench)..."
  cargo build --release -p tao-cli -p tao-bench
}

require_hyperfine() {
  if ! command -v hyperfine >/dev/null 2>&1; then
    echo "hyperfine is required for suite '${SUITE}'" >&2
    exit 1
  fi
}

prepare_fixture() {
  if [[ "${SKIP_GENERATE}" -eq 0 ]]; then
    echo "Generating deterministic fixtures (profile=${FIXTURE_PROFILE}, seed=${SEED})..."
    ./scripts/fixtures.sh --profile "${FIXTURE_PROFILE}" --seed "${SEED}" --output "${FIXTURE_ROOT}"
  fi

  if [[ ! -d "${FIXTURE_VAULT}" ]]; then
    echo "fixture vault not found: ${FIXTURE_VAULT}" >&2
    exit 1
  fi
  if [[ ! -f "${FIXTURE_VAULT}/${SAMPLE_NOTE}" ]]; then
    echo "sample note missing: ${FIXTURE_VAULT}/${SAMPLE_NOTE}" >&2
    exit 1
  fi
  if [[ ! -f "${FIXTURE_VAULT}/${SAMPLE_BASE}" ]]; then
    echo "sample base missing: ${FIXTURE_VAULT}/${SAMPLE_BASE}" >&2
    exit 1
  fi

  FIXTURE_VAULT="$(cd "${FIXTURE_VAULT}" && pwd -P)"
  DB_PATH="${FIXTURE_VAULT}/.tao/index.sqlite"
  DAEMON_SOCKET="${FIXTURE_VAULT}/.tao/taod.sock"

  echo "Seeding index for CLI benchmarks..."
  "${CLI_BIN}" --json vault open --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
  "${CLI_BIN}" --json vault reindex --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
}

start_daemon() {
  cleanup_daemon
  "${CLI_BIN}" --json vault daemon start --socket "${DAEMON_SOCKET}" >/dev/null
  DAEMON_RUNNING=1
}

run_tao_bench_scenario() {
  local scenario="$1"
  local iterations="$2"
  local report_path="$3"
  shift 3
  echo "Running tao-bench scenario=${scenario}..."
  "${BENCH_BIN}" \
    --scenario "${scenario}" \
    --iterations "${iterations}" \
    "$@" \
    --json-out "${report_path}"
}

validate_startup_budget() {
  bun --eval '
    const fs = require("node:fs");
    const reportPath = process.argv[1];
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const p95 = report?.latency?.p95_ms;
    const target = report?.budget?.target_p95_ms ?? 900;

    if (typeof p95 !== "number") {
      console.error("startup budget gate failed: missing latency.p95_ms in report");
      process.exit(1);
    }
    if (p95 > target) {
      console.error(`startup budget gate failed: p95 ${p95}ms exceeded target ${target}ms`);
      process.exit(1);
    }
    console.log(`startup budget gate passed: p95 ${p95}ms <= ${target}ms`);
  ' "${STARTUP_REPORT}"
}

run_baseline_cli_budgets() {
  require_hyperfine
  start_daemon

  echo "Running baseline query/docs hyperfine budget (daemon warm path)..."
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${HYPERFINE_QUERY_REPORT}" \
    "${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 > /dev/null"

  echo "Running baseline graph/unresolved hyperfine budget..."
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${HYPERFINE_GRAPH_REPORT}" \
    "${CLI_BIN} --json graph unresolved --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0 > /dev/null"

  bun --eval '
    const fs = require("node:fs");
    const [queryPath, graphPath, budget] = process.argv.slice(1);
    const query = JSON.parse(fs.readFileSync(queryPath, "utf8"));
    const graph = JSON.parse(fs.readFileSync(graphPath, "utf8"));
    const threshold = Number(budget);

    const queryMeanMs = (query.results?.[0]?.mean ?? 0) * 1000;
    const graphMeanMs = (graph.results?.[0]?.mean ?? 0) * 1000;

    if (queryMeanMs > threshold) {
      console.error(`query budget failed: ${queryMeanMs.toFixed(3)}ms > ${threshold}ms`);
      process.exit(1);
    }
    if (graphMeanMs > threshold) {
      console.error(`graph budget failed: ${graphMeanMs.toFixed(3)}ms > ${threshold}ms`);
      process.exit(1);
    }

    console.log(`query budget passed: ${queryMeanMs.toFixed(3)}ms <= ${threshold}ms`);
    console.log(`graph budget passed: ${graphMeanMs.toFixed(3)}ms <= ${threshold}ms`);
  ' "${HYPERFINE_QUERY_REPORT}" "${HYPERFINE_GRAPH_REPORT}" "${CLI_BUDGET_MS}"
}

run_daemon_query_benchmark() {
  require_hyperfine
  start_daemon

  echo "Running one-shot vs daemon query/docs benchmark..."
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${DAEMON_REPORT}" \
    "${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 > /dev/null" \
    "${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 > /dev/null"

  bun --eval '
    const fs = require("node:fs");
    const [reportPath, minImprovementRaw] = process.argv.slice(1);
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const [oneShot, daemon] = report.results ?? [];
    if (!oneShot || !daemon) {
      console.error("daemon benchmark missing one-shot or daemon result rows");
      process.exit(1);
    }
    const oneShotMeanMs = Number((oneShot.mean * 1000).toFixed(3));
    const daemonMeanMs = Number((daemon.mean * 1000).toFixed(3));
    const improvementPct = oneShotMeanMs <= 0
      ? 0
      : Number((((oneShotMeanMs - daemonMeanMs) / oneShotMeanMs) * 100).toFixed(2));
    const minImprovementPct = Number(minImprovementRaw);
    const summary = {
      one_shot_mean_ms: oneShotMeanMs,
      daemon_mean_ms: daemonMeanMs,
      improvement_pct: improvementPct,
      min_expected_improvement_pct: minImprovementPct,
      pass: improvementPct >= minImprovementPct,
    };
    const outPath = reportPath.replace(/\.json$/, ".summary.json");
    fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
    if (improvementPct < minImprovementPct) {
      console.error(
        `daemon improvement gate failed: ${improvementPct}% < ${minImprovementPct}% (one-shot ${oneShotMeanMs}ms vs daemon ${daemonMeanMs}ms)`
      );
      process.exit(1);
    }
    console.log(
      `daemon improvement gate passed: ${improvementPct}% >= ${minImprovementPct}% (one-shot ${oneShotMeanMs}ms vs daemon ${daemonMeanMs}ms)`
    );
  ' "${DAEMON_REPORT}" "${DAEMON_MIN_IMPROVEMENT_PCT}"
}

measure_peak_rss_kb() {
  local cmd="$1"
  local rss_output=""
  if /usr/bin/time -l true >/dev/null 2>&1; then
    rss_output=$({ /usr/bin/time -l bash -lc "$cmd" >/dev/null; } 2>&1 || true)
    echo "${rss_output}" | awk '/maximum resident set size/ { print $1; exit }'
  else
    rss_output=$({ /usr/bin/time -f '%M' bash -lc "$cmd" >/dev/null; } 2>&1 || true)
    echo "${rss_output}" | tail -n 1 | tr -d ' '
  fi
}

run_query_stream_projection_benchmark() {
  require_hyperfine

  start_daemon
  local loop_count=20
  local standard_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 1000 --offset 0 > /dev/null; done"
  local projected_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --select path --limit 1000 --offset 0 > /dev/null; done"
  local standard_rss_cmd="${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 1000 --offset 0 > /dev/null"
  local projected_rss_cmd="${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --select path --limit 1000 --offset 0 > /dev/null"

  echo "Running docs standard vs projected-column comparison..."
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${STREAM_COMPARE_REPORT}" \
    "${standard_cmd}" \
    "${projected_cmd}"

  local standard_rss_kb
  local stream_rss_kb
  standard_rss_kb="$(measure_peak_rss_kb "${standard_rss_cmd}")"
  stream_rss_kb="$(measure_peak_rss_kb "${projected_rss_cmd}")"

  bun --eval '
    const fs = require("node:fs");
    const percentile = (samples, p) => {
      if (!samples.length) return 0;
      const sorted = [...samples].sort((a, b) => a - b);
      const idx = Math.min(sorted.length - 1, Math.round((p / 100) * (sorted.length - 1)));
      return sorted[idx];
    };

    const [reportPath, summaryPath, minImprovementRaw, standardRssRaw, streamRssRaw] = process.argv.slice(1);
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const [standard, streaming] = report.results ?? [];
    if (!standard || !streaming) {
      console.error("stream comparison missing result rows");
      process.exit(1);
    }
    const standardTimes = standard.times ?? [];
    const streamingTimes = streaming.times ?? [];
    const standardP50 = percentile(standardTimes, 50) * 1000;
    const streamingP50 = percentile(streamingTimes, 50) * 1000;
    const improvementPct = standardP50 <= 0 ? 0 : ((standardP50 - streamingP50) / standardP50) * 100;
    const rssTolerancePct = 1;
    const rssDeltaPct = Number(standardRssRaw) <= 0
      ? 0
      : ((Number(streamRssRaw) - Number(standardRssRaw)) / Number(standardRssRaw)) * 100;
    const rssPass = rssDeltaPct <= rssTolerancePct;
    const summary = {
      standard: {
        mean_ms: Number(((standard.mean ?? 0) * 1000).toFixed(3)),
        p50_ms: Number(standardP50.toFixed(3)),
        p95_ms: Number((percentile(standardTimes, 95) * 1000).toFixed(3)),
        peak_rss_kb: Number(standardRssRaw),
      },
      projection: {
        mean_ms: Number(((streaming.mean ?? 0) * 1000).toFixed(3)),
        p50_ms: Number(streamingP50.toFixed(3)),
        p95_ms: Number((percentile(streamingTimes, 95) * 1000).toFixed(3)),
        peak_rss_kb: Number(streamRssRaw),
      },
      improvement_pct: Number(improvementPct.toFixed(2)),
      min_expected_improvement_pct: Number(minImprovementRaw),
      rss_delta_pct: Number(rssDeltaPct.toFixed(3)),
      rss_tolerance_pct: rssTolerancePct,
      pass: improvementPct >= Number(minImprovementRaw) && rssPass,
    };
    fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
    if (improvementPct < Number(minImprovementRaw)) {
      console.warn(`projection performance warning: ${improvementPct.toFixed(2)}% < ${minImprovementRaw}%`);
    }
    if (!rssPass) {
      console.warn(`projection RSS warning: delta ${rssDeltaPct.toFixed(3)}% exceeds tolerance ${rssTolerancePct}%`);
    }
    console.log(
      `projection comparison complete: p50 improvement ${improvementPct.toFixed(2)}%, RSS projected=${streamRssRaw}KB baseline=${standardRssRaw}KB`
    );
  ' "${STREAM_COMPARE_REPORT}" "${STREAM_COMPARE_SUMMARY}" "${STREAM_MIN_IMPROVEMENT_PCT}" "${standard_rss_kb}" "${stream_rss_kb}"
}

cli_matrix_benchmark() {
  local id="$1"
  local cmd="$2"
  local report_path="${CLI_MATRIX_REPORT_DIR}/${id}.json"
  echo "benchmarking ${id}"
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${report_path}" \
    "${cmd}"
}

run_cli_matrix() {
  require_hyperfine

  COMMAND_MATRIX=$(cat <<EOF
vault-stats|${CLI_BIN} --json vault stats --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
vault-preflight|${CLI_BIN} --json vault preflight --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
doc-read|${CLI_BIN} --json doc read --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
doc-list|${CLI_BIN} --json doc list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
base-list|${CLI_BIN} --json base list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
base-schema|${CLI_BIN} --json base schema --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path-or-id ${SAMPLE_BASE}
base-view|${CLI_BIN} --json base view --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path-or-id ${SAMPLE_BASE} --view-name ${SAMPLE_VIEW} --page 1 --page-size 50
graph-outgoing|${CLI_BIN} --json graph outgoing --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
graph-backlinks|${CLI_BIN} --json graph backlinks --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
graph-unresolved|${CLI_BIN} --json graph unresolved --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-deadends|${CLI_BIN} --json graph deadends --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-orphans|${CLI_BIN} --json graph orphans --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-components|${CLI_BIN} --json graph components --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-walk|${CLI_BIN} --json graph walk --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --depth 2 --limit 200
meta-properties|${CLI_BIN} --json meta properties --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-tags|${CLI_BIN} --json meta tags --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-aliases|${CLI_BIN} --json meta aliases --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-tasks|${CLI_BIN} --json meta tasks --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
task-list|${CLI_BIN} --json task list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
query-docs|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0
query-graph|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from graph --limit 50 --offset 0
query-graph-path|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from graph --path ${SAMPLE_NOTE} --limit 50 --offset 0
query-task|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from task --query follow --limit 50 --offset 0
query-meta-tags|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:tags --limit 50 --offset 0
query-meta-aliases|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:aliases --limit 50 --offset 0
query-meta-properties|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:properties --limit 50 --offset 0
query-base|${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from base:${SAMPLE_BASE} --view-name ${SAMPLE_VIEW} --limit 50 --offset 0
EOF
)

  while IFS="|" read -r id cmd; do
    [[ -z "${id}" ]] && continue
    cli_matrix_benchmark "${id}" "${cmd}"
  done <<< "${COMMAND_MATRIX}"

  bun --eval '
    const fs = require("node:fs");
    const path = require("node:path");
    const [reportDir, summaryPath, profile, runs, warmup] = process.argv.slice(1);
    const files = fs
      .readdirSync(reportDir)
      .filter((file) => file.endsWith(".json") && file !== "summary.json");
    const commands = files
      .map((file) => {
        const payload = JSON.parse(fs.readFileSync(path.join(reportDir, file), "utf8"));
        const result = payload.results?.[0] ?? {};
        return {
          id: file.replace(/\.json$/, ""),
          mean_ms: Number(((result.mean ?? 0) * 1000).toFixed(3)),
          stddev_ms: Number(((result.stddev ?? 0) * 1000).toFixed(3)),
          min_ms: Number(((result.min ?? 0) * 1000).toFixed(3)),
          max_ms: Number(((result.max ?? 0) * 1000).toFixed(3)),
        };
      })
      .sort((a, b) => a.mean_ms - b.mean_ms);
    const summary = {
      generated_at: new Date().toISOString(),
      profile,
      runs: Number(runs),
      warmup: Number(warmup),
      commands,
    };
    fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
    console.log(`summary written to ${summaryPath}`);
  ' "${CLI_MATRIX_REPORT_DIR}" "${CLI_MATRIX_SUMMARY_PATH}" "${FIXTURE_PROFILE}" "${RUNS}" "${WARMUP}"

  echo "CLI matrix reports:"
  echo "  report_dir=${CLI_MATRIX_REPORT_DIR}"
  echo "  summary=${CLI_MATRIX_SUMMARY_PATH}"
}

run_sdk_suite() {
  run_tao_bench_scenario parse 500 "${PARSE_REPORT}"
  run_tao_bench_scenario resolve 500 "${RESOLVE_REPORT}" --bridge-notes 10000
  run_tao_bench_scenario search 500 "${SEARCH_REPORT}"
  run_tao_bench_scenario bridge 200 "${BRIDGE_REPORT}" --enforce-budgets --max-p50-ms 50 --max-p95-ms 120
  run_tao_bench_scenario ffi 200 "${FFI_REPORT}" --enforce-budgets --max-p50-ms 20 --max-p95-ms 60
  run_tao_bench_scenario startup 50 "${STARTUP_REPORT}" --bridge-notes 1000
  run_tao_bench_scenario graph-walk 100 "${GRAPH_WALK_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --graph-root "${SAMPLE_NOTE}" --graph-depth 2 --graph-limit 200
  run_tao_bench_scenario unified-query 100 "${UNIFIED_QUERY_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100
  validate_startup_budget
  run_baseline_cli_budgets
  run_daemon_query_benchmark
  run_query_stream_projection_benchmark
}

build_bins_if_needed

case "${SUITE}" in
  all)
    prepare_fixture
    run_sdk_suite
    run_cli_matrix
    ;;
  sdk)
    prepare_fixture
    run_sdk_suite
    ;;
  cli)
    prepare_fixture
    run_cli_matrix
    ;;
  daemon)
    prepare_fixture
    run_daemon_query_benchmark
    ;;
  graph-walk)
    prepare_fixture
    run_tao_bench_scenario graph-walk 100 "${GRAPH_WALK_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --graph-root "${SAMPLE_NOTE}" --graph-depth 2 --graph-limit 200
    ;;
  unified-query)
    prepare_fixture
    run_tao_bench_scenario unified-query 100 "${UNIFIED_QUERY_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100
    ;;
  bridge)
    run_tao_bench_scenario bridge 200 "${BRIDGE_REPORT}" --enforce-budgets --max-p50-ms 50 --max-p95-ms 120
    ;;
  ffi)
    run_tao_bench_scenario ffi 200 "${FFI_REPORT}" --enforce-budgets --max-p50-ms 20 --max-p95-ms 60
    ;;
  startup)
    run_tao_bench_scenario startup 50 "${STARTUP_REPORT}" --bridge-notes 1000
    validate_startup_budget
    ;;
  parse)
    run_tao_bench_scenario parse 500 "${PARSE_REPORT}"
    ;;
  resolve)
    run_tao_bench_scenario resolve 500 "${RESOLVE_REPORT}" --bridge-notes 10000
    ;;
  search)
    run_tao_bench_scenario search 500 "${SEARCH_REPORT}"
    ;;
esac

echo "Benchmark suite '${SUITE}' complete."
echo "Reports written under ${REPORT_DIR}"
