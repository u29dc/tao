#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-${TAO_HOME:-${TOOLS_HOME:-$HOME/.tools}/tao}}"
DIST_DIR="dist"
BUNDLE_PATH="${DIST_DIR}/tao-cli-bundle.tar.gz"

echo "Building release binaries..."
cargo build --workspace --release

echo "Installing binaries to ${OUT_DIR}..."
mkdir -p "${OUT_DIR}"
cp target/release/tao "${OUT_DIR}/tao"
cp target/release/tao-tui "${OUT_DIR}/tao-tui"
chmod +x "${OUT_DIR}/tao" "${OUT_DIR}/tao-tui"

echo "Creating release bundle ${BUNDLE_PATH}..."
mkdir -p "${DIST_DIR}"
tar -C "${OUT_DIR}" -czf "${BUNDLE_PATH}" tao tao-tui

echo "Validating installed CLI binary..."
"${OUT_DIR}/tao" --help >/dev/null

echo "CLI release package ready:"
echo "  install_dir=${OUT_DIR}"
echo "  bundle=${BUNDLE_PATH}"
