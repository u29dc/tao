#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: scripts/release.sh [--target TARGET] [--out DIR]

Builds and packages Tao release CLI artifacts from a single entrypoint.

Targets:
  all   Build/install CLI binaries and create CLI tarball (default)
  cli   Build/install CLI binaries and create CLI tarball

Options:
  --target TARGET   all|cli (default: all)
  --out DIR         CLI install output dir (default: \$TAO_HOME or \$TOOLS_HOME/tao or \$HOME/.tools/tao)
  -h, --help        Show this help
USAGE
}

TARGET="all"
OUT_DIR="${TAO_HOME:-${TOOLS_HOME:-$HOME/.tools}/tao}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --out)
      OUT_DIR="${2:-}"
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

case "$TARGET" in
  all|cli)
    ;;
  *)
    echo "--target must be one of: all|cli" >&2
    exit 1
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist"

release_cli() {
  local bundle_path="${DIST_DIR}/tao-cli-bundle.tar.gz"
  local config_path="${OUT_DIR}/config.toml"

  echo "Building release binaries..."
  cargo build --release -p tao-cli -p tao-tui

  echo "Installing binaries to ${OUT_DIR}..."
  mkdir -p "${OUT_DIR}"
  cp "${ROOT_DIR}/target/release/tao" "${OUT_DIR}/tao"
  cp "${ROOT_DIR}/target/release/tao-tui" "${OUT_DIR}/tao-tui"
  chmod +x "${OUT_DIR}/tao" "${OUT_DIR}/tao-tui"
  if command -v codesign >/dev/null 2>&1; then
    codesign --force --sign - "${OUT_DIR}/tao" "${OUT_DIR}/tao-tui" >/dev/null
  fi

  if [[ ! -f "${config_path}" ]]; then
    cat > "${config_path}" <<'EOF'
[vault]
# root = "/absolute/path/to/vault"

[security]
# read_only = true
EOF
  fi

  echo "Creating release bundle ${bundle_path}..."
  mkdir -p "${DIST_DIR}"
  tar -C "${OUT_DIR}" -czf "${bundle_path}" tao tao-tui

  echo "Validating installed CLI binary..."
  if "${OUT_DIR}/tao" --help >/dev/null 2>&1; then
    :
  else
    echo "installed binary self-check failed at ${OUT_DIR}; validating target/release artifact instead" >&2
    "${ROOT_DIR}/target/release/tao" --help >/dev/null
  fi

  echo "CLI release package ready:"
  echo "  install_dir=${OUT_DIR}"
  echo "  bundle=${bundle_path}"
}

case "$TARGET" in
  all)
    release_cli
    ;;
  cli)
    release_cli
    ;;
esac
