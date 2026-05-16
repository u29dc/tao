#!/usr/bin/env bash
set -euo pipefail

ROOT="$(./scripts/path-guards.sh --repo-root)"
TMP_DIR="${ROOT}/.tmp-path-guards-test"
EXTERNAL_DIR="$(mktemp -d)"
mkdir -p "${TMP_DIR}"
cleanup() {
  rm -rf "${TMP_DIR}"
  rm -rf "${EXTERNAL_DIR}"
}
trap cleanup EXIT

./scripts/path-guards.sh --assert-repo-path "${ROOT}/vault/generated"
./scripts/path-guards.sh --assert-repo-path "${ROOT}/.tmp-path-guards-test/missing/nested"
./scripts/path-guards.sh --assert-existing-vault-path "${EXTERNAL_DIR}"

if ./scripts/path-guards.sh --assert-repo-path "/tmp" >/dev/null 2>&1; then
  echo "expected non-repo path assertion to fail" >&2
  exit 1
fi

if ./scripts/path-guards.sh --assert-existing-vault-path "${EXTERNAL_DIR}/missing" >/dev/null 2>&1; then
  echo "expected missing vault path assertion to fail" >&2
  exit 1
fi

if ./scripts/path-guards.sh --assert-existing-vault-path "/" >/dev/null 2>&1; then
  echo "expected root vault path assertion to fail" >&2
  exit 1
fi

if [[ -n "${HOME:-}" ]] && ./scripts/path-guards.sh --assert-existing-vault-path "${HOME}" >/dev/null 2>&1; then
  echo "expected home vault path assertion to fail" >&2
  exit 1
fi

ln -s "${EXTERNAL_DIR}" "${TMP_DIR}/escape-link"
if ./scripts/path-guards.sh --assert-repo-path "${TMP_DIR}/escape-link/nested" >/dev/null 2>&1; then
  echo "expected symlink escape assertion to fail" >&2
  exit 1
fi

echo "path_guards_test: ok"
