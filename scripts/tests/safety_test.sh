#!/usr/bin/env bash
set -euo pipefail

ROOT="$(./scripts/safety.sh --repo-root)"
TMP_DIR="${ROOT}/.tmp-safety-test"
LEAK_FILE="${ROOT}/scripts/tmp_safety_leak.txt"
mkdir -p "${TMP_DIR}"
cleanup() {
  rm -rf "${TMP_DIR}"
  rm -f "${LEAK_FILE}"
}
trap cleanup EXIT

./scripts/safety.sh --assert-path "${ROOT}/vault/generated"

if ./scripts/safety.sh --assert-path "/Users/han/Library/CloudStorage/Dropbox/VAULT" >/dev/null 2>&1; then
  echo "expected Dropbox path assertion to fail" >&2
  exit 1
fi

if ./scripts/safety.sh --assert-path "/tmp" >/dev/null 2>&1; then
  echo "expected non-repo path assertion to fail" >&2
  exit 1
fi

printf 'sentinel /Users/han/Dropbox/VAULT\n' > "${LEAK_FILE}"

if ./scripts/safety.sh --check-repo >/dev/null 2>&1; then
  echo "expected repository scan to fail when leak marker exists" >&2
  exit 1
fi

rm -f "${LEAK_FILE}"
./scripts/safety.sh --check-repo

echo "safety_test: ok"
