#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR=".benchmarks/reports"
BRIDGE_REPORT="${REPORT_DIR}/bridge-call-budgets.json"
FFI_REPORT="${REPORT_DIR}/ffi-call-budgets.json"
STARTUP_REPORT="${REPORT_DIR}/startup-budgets.json"
HYPERFINE_QUERY_REPORT="${REPORT_DIR}/query-docs-hyperfine.json"
HYPERFINE_GRAPH_REPORT="${REPORT_DIR}/graph-unresolved-hyperfine.json"
BENCH_BIN="target/release/tao-bench"
CLI_BIN="target/release/tao"
FIXTURE_ROOT="vault/generated"
FIXTURE_VAULT="${FIXTURE_ROOT}/vault-10k"

mkdir -p "${REPORT_DIR}"

if [ ! -x "${BENCH_BIN}" ]; then
  echo "Building release benchmark binary..."
  cargo build --release -p tao-bench
fi

if [ ! -x "${CLI_BIN}" ]; then
  echo "Building release CLI binary..."
  cargo build --release -p tao-cli
fi

echo "Generating deterministic fixtures..."
./scripts/fixtures.sh --profile 10k --seed 42 --output "${FIXTURE_ROOT}"
./scripts/validate_fixtures.sh "${FIXTURE_ROOT}"

echo "Seeding index for benchmark fixture vault..."
"${CLI_BIN}" --json vault open --vault-root "${FIXTURE_VAULT}" >/dev/null
"${CLI_BIN}" --json vault reindex --vault-root "${FIXTURE_VAULT}" >/dev/null

echo "Running bridge latency budget gate..."
"${BENCH_BIN}" \
  --scenario bridge \
  --iterations 200 \
  --enforce-budgets \
  --max-p50-ms 50 \
  --max-p95-ms 120 \
  --json-out "${BRIDGE_REPORT}"

echo "Running ffi latency budget gate..."
"${BENCH_BIN}" \
  --scenario ffi \
  --iterations 200 \
  --enforce-budgets \
  --max-p50-ms 20 \
  --max-p95-ms 60 \
  --json-out "${FFI_REPORT}"

echo "Running startup latency budget gate..."
"${BENCH_BIN}" \
  --scenario startup \
  --iterations 50 \
  --bridge-notes 1000 \
  --json-out "${STARTUP_REPORT}"

echo "Validating startup p95 budget..."
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

if command -v hyperfine >/dev/null 2>&1; then
  echo "Running query/docs hyperfine budget..."
  hyperfine \
    --warmup 5 \
    --runs 25 \
    --export-json "${HYPERFINE_QUERY_REPORT}" \
    "${CLI_BIN} --json query --vault-root ${FIXTURE_VAULT} --from docs --query project --limit 50 --offset 0 > /dev/null"

  echo "Running graph/unresolved hyperfine budget..."
  hyperfine \
    --warmup 5 \
    --runs 25 \
    --export-json "${HYPERFINE_GRAPH_REPORT}" \
    "${CLI_BIN} --json graph unresolved --vault-root ${FIXTURE_VAULT} --limit 50 --offset 0 > /dev/null"

  echo "Validating hyperfine p50 budgets..."
  bun --eval '
    const fs = require("node:fs");
    const [queryPath, graphPath] = process.argv.slice(1);
    const query = JSON.parse(fs.readFileSync(queryPath, "utf8"));
    const graph = JSON.parse(fs.readFileSync(graphPath, "utf8"));

    const queryMeanMs = (query.results?.[0]?.mean ?? 0) * 1000;
    const graphMeanMs = (graph.results?.[0]?.mean ?? 0) * 1000;
    const queryBudgetMs = 10;
    const graphBudgetMs = 10;

    if (queryMeanMs > queryBudgetMs) {
      console.error(`query budget failed: ${queryMeanMs.toFixed(3)}ms > ${queryBudgetMs}ms`);
      process.exit(1);
    }
    if (graphMeanMs > graphBudgetMs) {
      console.error(`graph budget failed: ${graphMeanMs.toFixed(3)}ms > ${graphBudgetMs}ms`);
      process.exit(1);
    }

    console.log(`query budget passed: ${queryMeanMs.toFixed(3)}ms <= ${queryBudgetMs}ms`);
    console.log(`graph budget passed: ${graphMeanMs.toFixed(3)}ms <= ${graphBudgetMs}ms`);
  ' "${HYPERFINE_QUERY_REPORT}" "${HYPERFINE_GRAPH_REPORT}"
else
  echo "hyperfine not installed; skipping query/graph CLI budgets" >&2
fi
