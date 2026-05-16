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
    echo "path-guards: python3 is required for canonical path checks" >&2
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

is_repo_local_path() {
  local path="${1:-}"
  if [[ "$path" != /* ]]; then
    path="${PWD}/${path}"
  fi
  [[ "$path" == "$REPO_ROOT"/* ]] || [[ "$path" == "$REPO_ROOT" ]]
}

assert_repo_local_path() {
  local raw_path="${1:-}"
  local label="${2:-path}"
  if [[ -z "$raw_path" ]]; then
    echo "path-guards: missing ${label}" >&2
    exit 1
  fi

  local path
  path="$(canonicalize_path "$raw_path")"

  if ! is_repo_local_path "$path"; then
    echo "path-guards: ${label} must be inside repository root (${REPO_ROOT}), got: $path" >&2
    exit 1
  fi
}

assert_existing_vault_path() {
  local raw_path="${1:-}"
  local label="${2:-vault path}"
  if [[ -z "$raw_path" ]]; then
    echo "path-guards: missing ${label}" >&2
    exit 1
  fi

  local path
  path="$(canonicalize_path "$raw_path")"

  if [[ ! -d "$path" ]]; then
    echo "path-guards: ${label} must be an existing directory, got: $path" >&2
    exit 1
  fi
  if [[ "$path" == "/" || ( -n "${HOME:-}" && "$path" == "$HOME" ) ]]; then
    echo "path-guards: refusing broad ${label}: $path" >&2
    exit 1
  fi
}

usage() {
  cat <<USAGE
Usage: scripts/path-guards.sh [--assert-repo-path PATH] [--assert-existing-vault-path PATH] [--repo-root]

Options:
  --assert-repo-path PATH
                        Fail unless PATH canonicalizes inside the repository root.
  --assert-existing-vault-path PATH
                        Fail unless PATH is an existing non-broad vault directory.
  --repo-root           Print repository root path.
USAGE
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ $# -eq 0 ]]; then
    usage
    exit 0
  fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --assert-repo-path)
        assert_repo_local_path "${2:-}" "path"
        shift 2
        ;;
      --assert-existing-vault-path)
        assert_existing_vault_path "${2:-}" "vault path"
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
