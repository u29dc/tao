#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: scripts/budgets.sh [--profile PROFILE] [--seed N] [--runs N] [--warmup N] [--output DIR] [--budget-config PATH] [--budget-ms N] [--skip-generate]

Phase23 read-budget gate for generated fixtures only.

Checks (daemon warm path, profile 10k by default):
  - query docs
  - query base
  - query graph
  - graph walk
  - meta tags

The gate fails when any command p50 exceeds --budget-ms (default 10).
USAGE
}

PROFILE=""
SEED="42"
RUNS="20"
WARMUP="5"
OUTPUT_ROOT=".benchmarks/reports"
BUDGET_MS=""
BUDGET_CONFIG="plan/perf-budgets.json"
SKIP_GENERATE=0
FIXTURE_ROOT="vault/generated"
FIXTURE_VAULT=""
DB_PATH=""
CLI_BIN="target/release/tao"
DAEMON_SOCKET=""
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_DIR=""
SUMMARY_JSON=""
SUMMARY_MD=""
DAEMON_RUNNING=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
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
    --budget-ms)
      BUDGET_MS="${2:-}"
      shift 2
      ;;
    --budget-config)
      BUDGET_CONFIG="${2:-}"
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

load_budget_defaults() {
  local config_path="$1"
  local fallback_profile="10k"
  local fallback_budget="10"
  if [[ ! -f "$config_path" ]]; then
    PROFILE="${PROFILE:-$fallback_profile}"
    BUDGET_MS="${BUDGET_MS:-$fallback_budget}"
    return
  fi

  local defaults
  defaults=$(bun --eval '
    const fs = require("node:fs");
    const [configPath, fallbackProfile, fallbackBudget] = process.argv.slice(1);
    let profile = fallbackProfile;
    let budget = fallbackBudget;
    try {
      const raw = JSON.parse(fs.readFileSync(configPath, "utf8"));
      if (typeof raw.profile === "string" && raw.profile.length > 0) {
        profile = raw.profile;
      }
      if (typeof raw.warm_read_p50_ms === "number" && Number.isFinite(raw.warm_read_p50_ms)) {
        budget = String(raw.warm_read_p50_ms);
      }
    } catch (_) {}
    process.stdout.write(`${profile}\n${budget}\n`);
  ' "$config_path" "$fallback_profile" "$fallback_budget")
  local default_profile default_budget
  default_profile="$(printf '%s\n' "$defaults" | sed -n '1p')"
  default_budget="$(printf '%s\n' "$defaults" | sed -n '2p')"
  PROFILE="${PROFILE:-$default_profile}"
  BUDGET_MS="${BUDGET_MS:-$default_budget}"
}

load_budget_defaults "${BUDGET_CONFIG}"

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
if ! [[ "$BUDGET_MS" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "--budget-ms must be a number" >&2
  exit 1
fi
case "$PROFILE" in
  1k|5k|10k|25k)
    ;;
  *)
    echo "--profile must be one of: 1k|5k|10k|25k" >&2
    exit 1
    ;;
esac

if ! command -v hyperfine >/dev/null 2>&1; then
  echo "hyperfine is required" >&2
  exit 1
fi

FIXTURE_VAULT="${FIXTURE_ROOT}/vault-${PROFILE}"
REPORT_DIR="${OUTPUT_ROOT}/${RUN_STAMP}/budgets"
SUMMARY_JSON="${REPORT_DIR}/summary.json"
SUMMARY_MD="${REPORT_DIR}/summary.md"
mkdir -p "${REPORT_DIR}"
ln -sfn "${RUN_STAMP}" "${OUTPUT_ROOT}/latest"

cleanup_daemon() {
  if [[ "${DAEMON_RUNNING}" -eq 1 ]]; then
    "${CLI_BIN}" --json vault daemon stop --socket "${DAEMON_SOCKET}" >/dev/null 2>&1 || true
    DAEMON_RUNNING=0
  fi
}

trap cleanup_daemon EXIT

build_cli_if_needed() {
  echo "Building release tao binary..."
  cargo build --release -p tao-cli >/dev/null
}

prepare_fixture() {
  if [[ "${SKIP_GENERATE}" -eq 0 ]]; then
    echo "Generating fixtures profile=${PROFILE} seed=${SEED}..."
    ./scripts/fixtures.sh --profile "${PROFILE}" --seed "${SEED}" --output "${FIXTURE_ROOT}"
  fi

  if [[ ! -d "${FIXTURE_VAULT}" ]]; then
    echo "fixture vault not found: ${FIXTURE_VAULT}" >&2
    exit 1
  fi

  FIXTURE_VAULT="$(cd "${FIXTURE_VAULT}" && pwd -P)"
  DB_PATH="${FIXTURE_VAULT}/.tao/index.sqlite"
  DAEMON_SOCKET="${FIXTURE_VAULT}/.tao/taod-budgets.sock"

  "${CLI_BIN}" --json vault open --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
  "${CLI_BIN}" --json vault reindex --vault-root "${FIXTURE_VAULT}" --db-path "${DB_PATH}" >/dev/null
}

start_daemon() {
  cleanup_daemon
  "${CLI_BIN}" --json vault daemon start --socket "${DAEMON_SOCKET}" >/dev/null
  DAEMON_RUNNING=1
}

run_case() {
  local id="$1"
  local cmd="$2"
  local report_path="${REPORT_DIR}/${id}.json"
  echo "budget benchmark ${id}"
  hyperfine \
    --warmup "${WARMUP}" \
    --runs "${RUNS}" \
    --export-json "${report_path}" \
    "${cmd}"
}

build_cli_if_needed
prepare_fixture
start_daemon

SAMPLE_NOTE="notes/projects/project-1.md"
SAMPLE_BASE="views/projects.base"
SAMPLE_VIEW="Projects"

CASE_MATRIX=$(cat <<EOF
query-docs|${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from docs --query project --select path,title --limit 1000 --offset 0 > /dev/null
query-base|${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from base:${SAMPLE_BASE} --view-name ${SAMPLE_VIEW} --limit 100 --offset 0 > /dev/null
query-graph|${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} query --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --from graph --path ${SAMPLE_NOTE} --limit 100 --offset 0 > /dev/null
graph-walk|${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} graph walk --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --path ${SAMPLE_NOTE} --depth 2 --limit 200 > /dev/null
meta-tags|${CLI_BIN} --json --daemon-socket ${DAEMON_SOCKET} meta tags --vault-root ${FIXTURE_VAULT} --db-path ${DB_PATH} --limit 100 --offset 0 > /dev/null
EOF
)

while IFS="|" read -r id cmd; do
  [[ -z "${id}" ]] && continue
  run_case "${id}" "${cmd}"
done <<< "${CASE_MATRIX}"

bun --eval '
  const fs = require("node:fs");
  const path = require("node:path");

  const [reportDir, summaryJsonPath, summaryMdPath, budgetRaw] = process.argv.slice(1);
  const budgetMs = Number(budgetRaw);
  const files = fs
    .readdirSync(reportDir)
    .filter((file) => file.endsWith(".json") && file !== "summary.json");

  const percentile = (samples, p) => {
    if (!samples.length) return 0;
    const sorted = [...samples].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.round((p / 100) * (sorted.length - 1)));
    return sorted[idx];
  };

  const checks = files
    .map((file) => {
      const payload = JSON.parse(fs.readFileSync(path.join(reportDir, file), "utf8"));
      const result = payload.results?.[0] ?? {};
      const timesSec = result.times ?? [];
      const p50Ms = percentile(timesSec, 50) * 1000;
      const p95Ms = percentile(timesSec, 95) * 1000;
      return {
        id: file.replace(/\.json$/, ""),
        p50_ms: Number(p50Ms.toFixed(3)),
        p95_ms: Number(p95Ms.toFixed(3)),
        mean_ms: Number(((result.mean ?? 0) * 1000).toFixed(3)),
        pass: p50Ms <= budgetMs,
      };
    })
    .sort((a, b) => a.p50_ms - b.p50_ms);

  const status = checks.every((check) => check.pass) ? "pass" : "fail";
  const summary = {
    generated_at: new Date().toISOString(),
    budget_p50_ms: budgetMs,
    status,
    checks,
  };
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`);

  const markdown = [
    "# Phase23 Read Budget Report",
    "",
    `- generated_at: \`${summary.generated_at}\``,
    `- budget_p50_ms: \`${budgetMs}\``,
    `- status: \`${status}\``,
    "",
    "| command | p50_ms | p95_ms | mean_ms | status |",
    "| --- | ---: | ---: | ---: | --- |",
    ...checks.map((check) =>
      `| ${check.id} | ${check.p50_ms.toFixed(3)} | ${check.p95_ms.toFixed(3)} | ${check.mean_ms.toFixed(3)} | ${check.pass ? "pass" : "fail"} |`
    ),
    "",
  ].join("\n");
  fs.writeFileSync(summaryMdPath, `${markdown}\n`);

  if (status !== "pass") {
    console.error(`budget gate failed: one or more checks exceeded ${budgetMs}ms p50`);
    process.exit(1);
  }
  console.log(`budget gate passed: all checks <= ${budgetMs}ms p50`);
' "${REPORT_DIR}" "${SUMMARY_JSON}" "${SUMMARY_MD}" "${BUDGET_MS}"

echo "budget reports written under ${REPORT_DIR}"
