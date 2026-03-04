#!/usr/bin/env bash
set -euo pipefail

REPORT_DIR=".benchmarks/reports"
BRIDGE_REPORT="${REPORT_DIR}/bridge-call-budgets.json"
FFI_REPORT="${REPORT_DIR}/ffi-call-budgets.json"
STARTUP_REPORT="${REPORT_DIR}/startup-budgets.json"
BENCH_BIN="target/release/tao-bench"

mkdir -p "${REPORT_DIR}"

if [ ! -x "${BENCH_BIN}" ]; then
  echo "Building release benchmark binary..."
  cargo build --release -p tao-bench
fi

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
