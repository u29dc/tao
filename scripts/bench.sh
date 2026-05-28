#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "${SCRIPT_DIR}/path-guards.sh"

usage() {
  cat <<USAGE
Usage: scripts/bench.sh [--suite SUITE] [--profile PROFILE] [--seed N] [--runs N] [--warmup N] [--output DIR] [--skip-generate] [--live-vault PATH] [--live-commands PATH]

Unified benchmark driver for Tao SDK/internal-bridge/core/CLI workloads.

Suites:
  all      Run sdk + full read-only cli matrix, plus live vault benchmarks when configured (default)
  sdk      Run parse/resolve/search/bridge/startup/graph-walk/unified-query + baseline query/graph budgets
  cli      Run full read-only CLI command matrix
  live     Run real-vault read-only/index benchmark matrix
  drift    Run single-file drift reindex benchmark on generated fixtures
  fixtures Run fixture generation throughput benchmark (1k, 5k)
  daemon   Run one-shot vs daemon warm-runtime comparison
  graph-walk Run tao-bench graph-walk scenario
  unified-query Run tao-bench unified-query scenario
  bridge   Run bridge scenario only
  startup  Run startup scenario only
  parse    Run parse scenario only
  resolve  Run resolve scenario only
  search   Run search scenario only

Options:
  --profile PROFILE   Fixture profile for CLI workloads: 1k|5k (default: 5k)
  --seed N            Fixture seed (default: 42)
  --runs N            Hyperfine runs per command (default: 25)
  --warmup N          Hyperfine warmup runs per command (default: 5)
  --output DIR        Benchmark output root (default: .benchmarks/reports)
  --skip-generate     Reuse existing fixture and skip generation/validation
  --live-vault PATH   Existing vault directory for live read-only benchmark runs; also read from TAO_BENCH_LIVE_VAULT
  --live-commands PATH
                      Optional gitignored command file for private live search/path probes (default: .benchmarks/live-commands.txt)
  -h, --help          Show this help
USAGE
}

SUITE="all"
FIXTURE_PROFILE="5k"
SEED="42"
RUNS="25"
WARMUP="5"
OUTPUT_ROOT=".benchmarks/reports"
SKIP_GENERATE=0
FIXTURE_ROOT="vault/generated"
LIVE_VAULT="${TAO_BENCH_LIVE_VAULT:-}"
LIVE_COMMANDS_FILE="${TAO_BENCH_LIVE_COMMANDS:-.benchmarks/live-commands.txt}"
LIVE_DB_PATH="${TAO_BENCH_LIVE_DB_PATH:-}"
LIVE_SOCKET="${TAO_BENCH_LIVE_SOCKET:-}"
CLI_BUDGET_MS="10"
DAEMON_MIN_IMPROVEMENT_PCT="5"
DAEMON_LOOP_COUNT="50"
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
    --live-vault)
      LIVE_VAULT="${2:-}"
      shift 2
      ;;
    --live-commands)
      LIVE_COMMANDS_FILE="${2:-}"
      shift 2
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
  1k|5k)
    ;;
  *)
    echo "--profile must be one of: 1k|5k" >&2
    exit 1
    ;;
esac
case "$SUITE" in
  all|sdk|cli|live|drift|fixtures|daemon|graph-walk|unified-query|bridge|startup|parse|resolve|search|core)
    ;;
  *)
    echo "--suite must be one of: all|sdk|cli|live|drift|fixtures|daemon|graph-walk|unified-query|bridge|startup|parse|resolve|search|core" >&2
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
LIVE_REPORT_DIR="${REPORT_DIR}/live-readonly"
LIVE_SUMMARY_PATH="${LIVE_REPORT_DIR}/summary.json"
BENCH_BIN="target/release/tao-bench"
CLI_BIN="target/release/tao"
FIXTURE_VAULT="${FIXTURE_ROOT}/vault-${FIXTURE_PROFILE}"
DB_PATH=""
SAMPLE_NOTE="notes/projects/project-1.md"
SAMPLE_TARGET_NOTE="notes/projects/project-2.md"
SAMPLE_BASE="views/projects.base"
SAMPLE_VIEW="Projects"

PARSE_REPORT="${REPORT_DIR}/parse-bench.json"
RESOLVE_REPORT="${REPORT_DIR}/resolve-bench.json"
SEARCH_REPORT="${REPORT_DIR}/search-bench.json"
SEARCH_DOCS_REPORT="${REPORT_DIR}/search-docs-bench.json"
SEARCH_FILES_REPORT="${REPORT_DIR}/search-files-bench.json"
SEARCH_CONTEXT_REPORT="${REPORT_DIR}/search-context-bench.json"
SEARCH_PATH_CONTEXT_REPORT="${REPORT_DIR}/search-path-context-bench.json"
BRIDGE_REPORT="${REPORT_DIR}/bridge-call-budgets.json"
STARTUP_REPORT="${REPORT_DIR}/startup-budgets.json"
GRAPH_WALK_REPORT="${REPORT_DIR}/graph-walk-bench.json"
GRAPH_WALK_FOLDERS_REPORT="${REPORT_DIR}/graph-walk-folders-bench.json"
UNIFIED_QUERY_REPORT="${REPORT_DIR}/unified-query-bench.json"
HYPERFINE_QUERY_REPORT="${REPORT_DIR}/query-docs-hyperfine.json"
HYPERFINE_GRAPH_REPORT="${REPORT_DIR}/graph-unresolved-hyperfine.json"
DAEMON_REPORT="${REPORT_DIR}/daemon-query-docs-hyperfine.json"
STREAM_COMPARE_REPORT="${REPORT_DIR}/query-docs-stream-vs-standard-hyperfine.json"
STREAM_COMPARE_SUMMARY="${REPORT_DIR}/query-docs-stream-vs-standard.summary.json"
DRIFT_REPORT="${REPORT_DIR}/single-file-drift-reindex-hyperfine.json"
DRIFT_SUMMARY="${REPORT_DIR}/single-file-drift-reindex.summary.json"
DAEMON_SOCKET=""
DAEMON_RUNNING=0

assert_repo_local_path "${OUTPUT_ROOT}" "benchmark output root"
assert_repo_local_path "${FIXTURE_ROOT}" "fixture root"
assert_repo_local_path "${REPORT_DIR}" "benchmark report dir"
assert_repo_local_path "${CLI_MATRIX_REPORT_DIR}" "benchmark cli matrix report dir"
assert_repo_local_path "${OUTPUT_ROOT}/latest" "benchmark latest symlink"

mkdir -p "${REPORT_DIR}" "${CLI_MATRIX_REPORT_DIR}"
ln -sfn "${RUN_STAMP}" "${OUTPUT_ROOT}/latest"

cleanup_daemon() {
  if [[ "${DAEMON_RUNNING}" -eq 1 ]]; then
    "${CLI_BIN}" vault daemon stop --socket "${DAEMON_SOCKET}" >/dev/null 2>&1 || true
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
  if [[ ! -f "${FIXTURE_VAULT}/${SAMPLE_TARGET_NOTE}" ]]; then
    echo "sample target note missing: ${FIXTURE_VAULT}/${SAMPLE_TARGET_NOTE}" >&2
    exit 1
  fi
  if [[ ! -f "${FIXTURE_VAULT}/${SAMPLE_BASE}" ]]; then
    echo "sample base missing: ${FIXTURE_VAULT}/${SAMPLE_BASE}" >&2
    exit 1
  fi

  FIXTURE_VAULT="$(cd "${FIXTURE_VAULT}" && pwd -P)"
  assert_repo_local_path "${FIXTURE_VAULT}" "fixture vault"
  DB_PATH="${FIXTURE_VAULT}/.tao/index.sqlite"
  DAEMON_SOCKET="${FIXTURE_VAULT}/.tao/taod.sock"
  assert_repo_local_path "${DB_PATH}" "benchmark sqlite path"
  assert_repo_local_path "${DAEMON_SOCKET}" "daemon socket path"
  mkdir -p "$(dirname "${DB_PATH}")"

  echo "Seeding index for CLI benchmarks..."
  "${CLI_BIN}" vault daemon stop --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null 2>&1 || true
  "${CLI_BIN}" vault open --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
  "${CLI_BIN}" vault reindex --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
  if [[ ! -f "${DB_PATH}" ]]; then
    echo "benchmark sqlite path was not created: ${DB_PATH}" >&2
    exit 1
  fi
}

shell_quote() {
  printf '%q' "$1"
}

prepare_live_vault() {
  if [[ -z "${LIVE_VAULT}" ]]; then
    echo "live benchmark requires --live-vault PATH or TAO_BENCH_LIVE_VAULT" >&2
    exit 1
  fi

  assert_existing_vault_path "${LIVE_VAULT}" "live benchmark vault"
  LIVE_VAULT="$(cd "${LIVE_VAULT}" && pwd -P)"

  local tao_dir="${LIVE_VAULT}/.tao"
  mkdir -p "${tao_dir}"
  LIVE_DB_PATH="${LIVE_DB_PATH:-${tao_dir}/bench-index.sqlite}"
  LIVE_SOCKET="${LIVE_SOCKET:-${tao_dir}/taod-bench.sock}"
  case "${LIVE_DB_PATH}" in
    "${tao_dir}"/*) ;;
    *)
      echo "live benchmark db path must stay under ${tao_dir}" >&2
      exit 1
      ;;
  esac
  case "${LIVE_SOCKET}" in
    "${tao_dir}"/*) ;;
    *)
      echo "live benchmark socket path must stay under ${tao_dir}" >&2
      exit 1
      ;;
  esac

  echo "Preparing live vault benchmark index..."
  "${CLI_BIN}" vault daemon stop --socket "${LIVE_SOCKET}" >/dev/null 2>&1 || true
  "${CLI_BIN}" --daemon-socket "${LIVE_SOCKET}" vault open --vault-root "${LIVE_VAULT}" --db-path "${LIVE_DB_PATH}" >/dev/null
  "${CLI_BIN}" --daemon-socket "${LIVE_SOCKET}" vault reindex --vault-root "${LIVE_VAULT}" --db-path "${LIVE_DB_PATH}" >/dev/null
  if [[ ! -f "${LIVE_DB_PATH}" ]]; then
    echo "live benchmark sqlite path was not created: ${LIVE_DB_PATH}" >&2
    exit 1
  fi
}

start_daemon() {
  cleanup_daemon
  "${CLI_BIN}" vault daemon start --socket "${DAEMON_SOCKET}" >/dev/null
  DAEMON_RUNNING=1
}

start_live_daemon() {
  cleanup_daemon
  DAEMON_SOCKET="${LIVE_SOCKET}"
  "${CLI_BIN}" vault daemon start --socket "${DAEMON_SOCKET}" >/dev/null
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
    "${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 > /dev/null"

  echo "Running baseline graph/unresolved hyperfine budget..."
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${HYPERFINE_GRAPH_REPORT}" \
    "${CLI_BIN} graph unresolved --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0 > /dev/null"

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

  local loop_count="${DAEMON_LOOP_COUNT}"
  local one_shot_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 >/dev/null; done"
  local daemon_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0 >/dev/null; done"

  echo "Running one-shot vs daemon query/docs benchmark..."
  hyperfine \
    --shell=bash \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${DAEMON_REPORT}" \
    "${one_shot_cmd}" \
    "${daemon_cmd}"

  bun --eval '
    const fs = require("node:fs");
    const [reportPath, minImprovementRaw, loopCountRaw] = process.argv.slice(1);
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const [oneShot, daemon] = report.results ?? [];
    if (!oneShot || !daemon) {
      console.error("daemon benchmark missing one-shot or daemon result rows");
      process.exit(1);
    }
    const loopCount = Math.max(1, Number(loopCountRaw));
    const oneShotMeanMs = Number((((oneShot.mean * 1000) / loopCount)).toFixed(3));
    const daemonMeanMs = Number((((daemon.mean * 1000) / loopCount)).toFixed(3));
    const improvementPct = oneShotMeanMs <= 0
      ? 0
      : Number((((oneShotMeanMs - daemonMeanMs) / oneShotMeanMs) * 100).toFixed(2));
    const minImprovementPct = Number(minImprovementRaw);
    const summary = {
      one_shot_mean_ms: oneShotMeanMs,
      daemon_mean_ms: daemonMeanMs,
      iterations_per_sample: loopCount,
      improvement_pct: improvementPct,
      min_expected_improvement_pct: minImprovementPct,
      pass: improvementPct >= minImprovementPct,
    };
    const outPath = reportPath.replace(/\.json$/, ".summary.json");
    fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
    if (improvementPct < minImprovementPct) {
      console.warn(
        `daemon improvement below target: ${improvementPct}% < ${minImprovementPct}% (one-shot ${oneShotMeanMs}ms vs daemon ${daemonMeanMs}ms)`
      );
    } else {
      console.log(
        `daemon improvement target met: ${improvementPct}% >= ${minImprovementPct}% (one-shot ${oneShotMeanMs}ms vs daemon ${daemonMeanMs}ms)`
      );
    }
  ' "${DAEMON_REPORT}" "${DAEMON_MIN_IMPROVEMENT_PCT}" "${DAEMON_LOOP_COUNT}"
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
  local standard_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 1000 --offset 0 > /dev/null; done"
  local projected_cmd="for i in {1..${loop_count}}; do ${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --select path --limit 1000 --offset 0 > /dev/null; done"
  local standard_rss_cmd="${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 1000 --offset 0 > /dev/null"
  local projected_rss_cmd="${CLI_BIN} --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --select path --limit 1000 --offset 0 > /dev/null"

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

run_single_file_drift_benchmark() {
  require_hyperfine

  local note_path="${FIXTURE_VAULT}/${SAMPLE_NOTE}"
  local baseline_path="${REPORT_DIR}/single-file-drift-baseline.md"
  local note_q baseline_q tao_q vault_q db_q
  note_q="$(shell_quote "${note_path}")"
  baseline_q="$(shell_quote "${baseline_path}")"
  tao_q="$(shell_quote "${CLI_BIN}")"
  vault_q="$(shell_quote "${FIXTURE_VAULT}")"
  db_q="$(shell_quote "${DB_PATH}")"

  cp "${note_path}" "${baseline_path}"

  echo "Running single-file drift reindex benchmark..."
  hyperfine \
    --shell=bash \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --prepare "cp ${baseline_q} ${note_q} && ${tao_q} vault reindex --vault-root ${vault_q} --db-path ${db_q} > /dev/null" \
    --export-json "${DRIFT_REPORT}" \
    "printf '%s\n' '# Drift benchmark' '[[project-2]]' > ${note_q} && ${tao_q} vault reindex --vault-root ${vault_q} --db-path ${db_q} > /dev/null"

  cp "${baseline_path}" "${note_path}"
  "${CLI_BIN}" vault reindex --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null

  bun --eval '
    const fs = require("node:fs");
    const [reportPath, summaryPath, profile, runs, warmup] = process.argv.slice(1);
    const payload = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    const result = payload.results?.[0] ?? {};
    const summary = {
      generated_at: new Date().toISOString(),
      profile,
      runs: Number(runs),
      warmup: Number(warmup),
      mean_ms: Number(((result.mean ?? 0) * 1000).toFixed(3)),
      stddev_ms: Number(((result.stddev ?? 0) * 1000).toFixed(3)),
      min_ms: Number(((result.min ?? 0) * 1000).toFixed(3)),
      max_ms: Number(((result.max ?? 0) * 1000).toFixed(3)),
    };
    fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
    console.log(`single-file drift summary written to ${summaryPath}`);
  ' "${DRIFT_REPORT}" "${DRIFT_SUMMARY}" "${FIXTURE_PROFILE}" "${RUNS}" "${WARMUP}"
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
vault-stats|${CLI_BIN} vault stats --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
vault-preflight|${CLI_BIN} vault preflight --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
doc-read|${CLI_BIN} doc read --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
doc-list|${CLI_BIN} doc list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
base-list|${CLI_BIN} base list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH}
base-schema|${CLI_BIN} base schema --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path-or-id ${SAMPLE_BASE}
base-view|${CLI_BIN} base view --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path-or-id ${SAMPLE_BASE} --view-name ${SAMPLE_VIEW} --page 1 --page-size 50
graph-outgoing|${CLI_BIN} graph outgoing --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
graph-backlinks|${CLI_BIN} graph backlinks --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE}
graph-unresolved|${CLI_BIN} graph unresolved --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-deadends|${CLI_BIN} graph deadends --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-orphans|${CLI_BIN} graph orphans --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-components|${CLI_BIN} graph components --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50 --offset 0
graph-components-strong|${CLI_BIN} graph components --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --mode strong --limit 50 --offset 0
graph-neighbors|${CLI_BIN} graph neighbors --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --limit 100 --offset 0
graph-path|${CLI_BIN} graph path --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from ${SAMPLE_NOTE} --to ${SAMPLE_TARGET_NOTE} --max-depth 8 --max-nodes 10000
graph-walk|${CLI_BIN} graph walk --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --depth 2 --limit 200
graph-walk-folders|${CLI_BIN} graph walk --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --depth 2 --limit 200 --include-folders
meta-properties|${CLI_BIN} meta properties --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-tags|${CLI_BIN} meta tags --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-aliases|${CLI_BIN} meta aliases --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
meta-tasks|${CLI_BIN} meta tasks --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
task-list|${CLI_BIN} task list --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0
search-project|${CLI_BIN} search project --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 50
search-project-context|${CLI_BIN} search project --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --context --depth 2 --limit 20
search-project-path-context|${CLI_BIN} search --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --context --depth 2 --limit 20
query-docs|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --limit 50 --offset 0
query-docs-where|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --where "matched_in contains 'content' or matched_in contains 'title' or matched_in contains 'path'" --limit 50 --offset 0
query-docs-sort|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --sort title:asc --select path --limit 50 --offset 0
query-docs-where-sort|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --where "matched_in contains 'content' or matched_in contains 'title' or matched_in contains 'path'" --sort title:desc --select path --limit 50 --offset 0
query-graph|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from graph --limit 50 --offset 0
query-graph-path|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from graph --path ${SAMPLE_NOTE} --limit 50 --offset 0
query-task|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from task --query follow --limit 50 --offset 0
query-meta-tags|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:tags --limit 50 --offset 0
query-meta-aliases|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:aliases --limit 50 --offset 0
query-meta-properties|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from meta:properties --limit 50 --offset 0
query-base|${CLI_BIN} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from base:${SAMPLE_BASE} --view-name ${SAMPLE_VIEW} --limit 50 --offset 0
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

live_matrix_benchmark() {
  local id="$1"
  local cmd="$2"
  local report_path="${LIVE_REPORT_DIR}/${id}.json"
  echo "benchmarking live ${id}"
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${report_path}" \
    "${cmd}"
}

append_live_command_file() {
  local matrix_var_name="$1"
  [[ -f "${LIVE_COMMANDS_FILE}" ]] || return 0

  local tao_q vault_q db_q socket_q
  tao_q="$(shell_quote "${CLI_BIN}")"
  vault_q="$(shell_quote "${LIVE_VAULT}")"
  db_q="$(shell_quote "${LIVE_DB_PATH}")"
  socket_q="$(shell_quote "${LIVE_SOCKET}")"

  local line id cmd
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%%$'\r'}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    if [[ "${line}" != *"|"* ]]; then
      echo "invalid live command line in ${LIVE_COMMANDS_FILE}: expected id|command" >&2
      exit 1
    fi
    id="${line%%|*}"
    cmd="${line#*|}"
    if [[ ! "${id}" =~ ^[a-zA-Z0-9_.-]+$ ]]; then
      echo "invalid live command id '${id}' in ${LIVE_COMMANDS_FILE}" >&2
      exit 1
    fi
    cmd="${cmd//\{tao\}/${tao_q}}"
    cmd="${cmd//\{vault\}/${vault_q}}"
    cmd="${cmd//\{db\}/${db_q}}"
    cmd="${cmd//\{socket\}/${socket_q}}"
    printf -v "${matrix_var_name}" '%s\n%s|%s' "${!matrix_var_name}" "${id}" "${cmd}"
  done < "${LIVE_COMMANDS_FILE}"
}

run_live_matrix() {
  require_hyperfine
  prepare_live_vault
  start_live_daemon
  mkdir -p "${LIVE_REPORT_DIR}"

  local tao_q vault_q db_q socket_q
  tao_q="$(shell_quote "${CLI_BIN}")"
  vault_q="$(shell_quote "${LIVE_VAULT}")"
  db_q="$(shell_quote "${LIVE_DB_PATH}")"
  socket_q="$(shell_quote "${LIVE_SOCKET}")"

  LIVE_MATRIX=$(cat <<EOF
vault-preflight|${tao_q} vault preflight --vault-root ${vault_q} --db-path ${db_q} > /dev/null
vault-reindex-dry-run|${tao_q} vault reindex --vault-root ${vault_q} --db-path ${db_q} --dry-run > /dev/null
vault-reindex|${tao_q} vault reindex --vault-root ${vault_q} --db-path ${db_q} > /dev/null
health|${tao_q} health --vault-root ${vault_q} --db-path ${db_q} > /dev/null
graph-unresolved|${tao_q} --daemon-socket ${socket_q} graph audit --vault-root ${vault_q} --db-path ${db_q} --kind unresolved --limit 100 --offset 0 > /dev/null
graph-components|${tao_q} --daemon-socket ${socket_q} graph audit --vault-root ${vault_q} --db-path ${db_q} --kind components --limit 100 --offset 0 > /dev/null
search-broad-all|${tao_q} --daemon-socket ${socket_q} search project --vault-root ${vault_q} --db-path ${db_q} --kind all --limit 100 > /dev/null
search-docs|${tao_q} --daemon-socket ${socket_q} search meeting --vault-root ${vault_q} --db-path ${db_q} --kind docs --limit 50 > /dev/null
search-invoice-files|${tao_q} --daemon-socket ${socket_q} search invoice --vault-root ${vault_q} --db-path ${db_q} --kind files --limit 20 > /dev/null
search-pdf-files|${tao_q} --daemon-socket ${socket_q} search pdf --vault-root ${vault_q} --db-path ${db_q} --kind files --ext pdf --limit 20 > /dev/null
search-properties|${tao_q} --daemon-socket ${socket_q} search status --vault-root ${vault_q} --db-path ${db_q} --kind properties --limit 50 > /dev/null
search-graph|${tao_q} --daemon-socket ${socket_q} search meeting --vault-root ${vault_q} --db-path ${db_q} --kind graph --limit 50 > /dev/null
search-meeting-context|${tao_q} --daemon-socket ${socket_q} search meeting --vault-root ${vault_q} --db-path ${db_q} --context --depth 2 --limit 20 > /dev/null
search-task|${tao_q} --daemon-socket ${socket_q} search task --vault-root ${vault_q} --db-path ${db_q} --kind tasks --limit 50 > /dev/null
EOF
)

  append_live_command_file LIVE_MATRIX

  while IFS="|" read -r id cmd; do
    [[ -z "${id}" ]] && continue
    live_matrix_benchmark "${id}" "${cmd}"
  done <<< "${LIVE_MATRIX}"

  bun --eval '
    const fs = require("node:fs");
    const path = require("node:path");
    const [reportDir, summaryPath, runs, warmup, commandFile] = process.argv.slice(1);
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
      vault_kind: "live",
      runs: Number(runs),
      warmup: Number(warmup),
      private_command_file_loaded: fs.existsSync(commandFile),
      commands,
    };
    fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
    console.log(`live summary written to ${summaryPath}`);
  ' "${LIVE_REPORT_DIR}" "${LIVE_SUMMARY_PATH}" "${RUNS}" "${WARMUP}" "${LIVE_COMMANDS_FILE}"

  echo "Live benchmark reports:"
  echo "  report_dir=${LIVE_REPORT_DIR}"
  echo "  summary=${LIVE_SUMMARY_PATH}"
}

run_sdk_suite() {
  run_tao_bench_scenario parse 500 "${PARSE_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --bridge-notes 1000
  run_tao_bench_scenario resolve 500 "${RESOLVE_REPORT}" --bridge-notes 5000
  run_tao_bench_scenario search 500 "${SEARCH_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100
  run_tao_bench_scenario search 500 "${SEARCH_DOCS_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100 --kind docs
  run_tao_bench_scenario search 500 "${SEARCH_FILES_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100 --kind files --ext md
  run_tao_bench_scenario search 200 "${SEARCH_CONTEXT_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 20 --context --depth 2
  run_tao_bench_scenario search 200 "${SEARCH_PATH_CONTEXT_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-limit 20 --path "${SAMPLE_NOTE}" --context --depth 2
  run_tao_bench_scenario bridge 200 "${BRIDGE_REPORT}" --enforce-budgets --max-p50-ms 50 --max-p95-ms 120
  run_tao_bench_scenario startup 50 "${STARTUP_REPORT}" --bridge-notes 1000
  run_tao_bench_scenario graph-walk 100 "${GRAPH_WALK_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --graph-root "${SAMPLE_NOTE}" --graph-depth 2 --graph-limit 200
  run_tao_bench_scenario graph-walk 100 "${GRAPH_WALK_FOLDERS_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --graph-root "${SAMPLE_NOTE}" --graph-depth 2 --graph-limit 200 --graph-include-folders
  run_tao_bench_scenario unified-query 100 "${UNIFIED_QUERY_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100
  validate_startup_budget
  run_baseline_cli_budgets
  run_daemon_query_benchmark
  run_query_stream_projection_benchmark
  run_single_file_drift_benchmark
}

run_fixture_generation_benchmark() {
  require_hyperfine
  local report_path="${REPORT_DIR}/fixture-generation.summary.json"
  local budget_1k_ms="${FIXTURE_BUDGET_1K_MS:-0}"
  local budget_5k_ms="${FIXTURE_BUDGET_5K_MS:-0}"
  local profile
  for profile in 1k 5k; do
    local profile_report="${REPORT_DIR}/fixture-generation-${profile}.json"
    echo "benchmarking fixture generation ${profile}"
    hyperfine \
      --warmup 0 \
      --runs "${RUNS}" \
      --export-json "${profile_report}" \
      "./scripts/fixtures.sh --profile ${profile} --seed ${SEED} --output ${FIXTURE_ROOT} >/dev/null"
  done

  bun --eval '
    const fs = require("node:fs");
    const path = require("node:path");
    const [reportDir, reportPath, b1k, b5k] = process.argv.slice(1);
    const budgets = {
      "1k": Number(b1k),
      "5k": Number(b5k),
    };
    const profiles = [
      ["1k", 1000],
      ["5k", 5000],
    ];
    const rows = profiles.map(([profile, notesTotal]) => {
      const payload = JSON.parse(
        fs.readFileSync(path.join(reportDir, `fixture-generation-${profile}.json`), "utf8")
      );
      const result = payload.results?.[0] ?? {};
      const durationMs = Number(((result.mean ?? 0) * 1000).toFixed(3));
      const minMs = Number(((result.min ?? 0) * 1000).toFixed(3));
      const maxMs = Number(((result.max ?? 0) * 1000).toFixed(3));
      const notesPerSec = durationMs <= 0 ? 0 : Number(((notesTotal / durationMs) * 1000).toFixed(2));
      const budgetMs = budgets[profile] ?? 0;
      return {
        profile,
        notes_total: notesTotal,
        duration_ms: durationMs,
        min_ms: minMs,
        max_ms: maxMs,
        notes_per_sec: notesPerSec,
        budget_ms: budgetMs > 0 ? budgetMs : null,
        budget_pass: budgetMs > 0 ? durationMs <= budgetMs : null,
      };
    });
    const summary = {
      generated_at: new Date().toISOString(),
      rows,
    };
    fs.writeFileSync(reportPath, `${JSON.stringify(summary, null, 2)}\n`);
    const failed = rows.find((row) => row.budget_ms !== null && row.budget_pass === false);
    if (failed) {
      console.error(
        `fixture generation budget failed for ${failed.profile}: ${failed.duration_ms}ms > ${failed.budget_ms}ms`
      );
      process.exit(1);
    }
    console.log(`fixture generation summary written to ${reportPath}`);
  ' "${REPORT_DIR}" "${report_path}" "${budget_1k_ms}" "${budget_5k_ms}"
}

build_bins_if_needed

case "${SUITE}" in
  all)
    prepare_fixture
    run_sdk_suite
    run_cli_matrix
    if [[ -n "${LIVE_VAULT}" ]]; then
      run_live_matrix
    else
      echo "Skipping live benchmark: set TAO_BENCH_LIVE_VAULT or pass --live-vault."
    fi
    ;;
  sdk)
    prepare_fixture
    run_sdk_suite
    ;;
  cli)
    prepare_fixture
    run_cli_matrix
    ;;
  live)
    run_live_matrix
    ;;
  drift)
    prepare_fixture
    run_single_file_drift_benchmark
    ;;
  fixtures)
    run_fixture_generation_benchmark
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
  startup)
    run_tao_bench_scenario startup 50 "${STARTUP_REPORT}" --bridge-notes 1000
    validate_startup_budget
    ;;
  parse)
    prepare_fixture
    run_tao_bench_scenario parse 500 "${PARSE_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --bridge-notes 1000
    ;;
  resolve)
    run_tao_bench_scenario resolve 500 "${RESOLVE_REPORT}" --bridge-notes 5000
    ;;
  search)
    prepare_fixture
    run_tao_bench_scenario search 500 "${SEARCH_REPORT}" --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" --query-text project --query-limit 100
    ;;
esac

echo "Benchmark suite '${SUITE}' complete."
echo "Reports written under ${REPORT_DIR}"
