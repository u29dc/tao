#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

canonicalize_path() {
  local raw_path="${1:-}"
  if [[ -z "$raw_path" ]]; then
    echo "" >&2
    return 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "safety: python3 is required for canonical path checks" >&2
    exit 1
  fi
  python3 - "$raw_path" <<'PY'
import os
import sys

raw_path = sys.argv[1]
if not os.path.isabs(raw_path):
    raw_path = os.path.join(os.getcwd(), raw_path)
print(os.path.realpath(raw_path))
PY
}

is_forbidden_personal_path() {
  local path="${1:-}"
  [[ "$path" == /Users/han/Library/CloudStorage/Dropbox/* ]] || [[ "$path" == /Users/han/Dropbox/* ]]
}

is_repo_local_path() {
  local path="${1:-}"
  if [[ "$path" != /* ]]; then
    path="${PWD}/${path}"
  fi
  [[ "$path" == "$REPO_ROOT"/* ]] || [[ "$path" == "$REPO_ROOT" ]]
}

assert_safe_path() {
  local raw_path="${1:-}"
  local label="${2:-path}"
  if [[ -z "$raw_path" ]]; then
    echo "safety: missing ${label}" >&2
    exit 1
  fi

  local path
  path="$(canonicalize_path "$raw_path")"

  if is_forbidden_personal_path "$path"; then
    echo "safety: forbidden personal path for ${label}: $path" >&2
    exit 1
  fi

  if ! is_repo_local_path "$path"; then
    echo "safety: ${label} must be inside repository root (${REPO_ROOT}), got: $path" >&2
    exit 1
  fi
}

scan_repo_forbidden_literals() {
  local root="${1:-$REPO_ROOT}"
  local pattern='(/Users/han/Library/CloudStorage/Dropbox|/Users/han/Dropbox|Dropbox/VAULT)'
  local -a scan_paths=(
    "$root/scripts"
    "$root/package.json"
    "$root/Cargo.toml"
    "$root/crates"
    "$root/apps"
    "$root/.github"
    "$root/vault"
  )

  if rg -n "$pattern" "${scan_paths[@]}" \
    --glob '!.git/**' \
    --glob '!target/**' \
    --glob '!node_modules/**' \
    --glob '!.benchmarks/**' \
    --glob '!dist/**' \
    --glob '!plan/**' \
    --glob '!AGENTS.md' \
    --glob '!scripts/safety.sh' \
    --glob '!scripts/fixtures.sh' \
    --glob '!scripts/tests/safety_test.sh' >/dev/null 2>&1; then
    echo "safety: forbidden personal path marker detected in executable/config paths." >&2
    rg -n "$pattern" "${scan_paths[@]}" \
      --glob '!.git/**' \
      --glob '!target/**' \
      --glob '!node_modules/**' \
      --glob '!.benchmarks/**' \
      --glob '!dist/**' \
      --glob '!plan/**' \
      --glob '!AGENTS.md' \
      --glob '!scripts/safety.sh' \
      --glob '!scripts/fixtures.sh' \
      --glob '!scripts/tests/safety_test.sh' >&2
    exit 1
  fi
}

usage() {
  cat <<USAGE
Usage: scripts/safety.sh [--check-repo] [--assert-path PATH] [--repo-root]

Options:
  --check-repo         Fail if forbidden personal-path markers exist in repository files.
  --assert-path PATH   Fail unless PATH is repository-local and not a personal Dropbox path.
  --repo-root          Print repository root path.
USAGE
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -eq 0 ]]; then
    usage
    exit 0
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --check-repo)
        scan_repo_forbidden_literals "$REPO_ROOT"
        shift
        ;;
      --assert-path)
        assert_safe_path "${2:-}" "path"
        shift 2
        ;;
      --repo-root)
        echo "$REPO_ROOT"
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
fi
