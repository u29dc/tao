#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-${OBS_HOME:-${TOOLS_HOME:-$HOME/.tools}/obs}}"
DIST_DIR="dist"
BUNDLE_PATH="${DIST_DIR}/obs-cli-bundle.tar.gz"

echo "Building release binaries..."
cargo build --workspace --release

echo "Installing binaries to ${OUT_DIR}..."
mkdir -p "${OUT_DIR}"
cp target/release/obs "${OUT_DIR}/obs"
cp target/release/obs-tui "${OUT_DIR}/obs-tui"
chmod +x "${OUT_DIR}/obs" "${OUT_DIR}/obs-tui"

echo "Creating release bundle ${BUNDLE_PATH}..."
mkdir -p "${DIST_DIR}"
tar -C "${OUT_DIR}" -czf "${BUNDLE_PATH}" obs obs-tui

echo "Validating installed CLI binary..."
"${OUT_DIR}/obs" --help >/dev/null

echo "CLI release package ready:"
echo "  install_dir=${OUT_DIR}"
echo "  bundle=${BUNDLE_PATH}"
