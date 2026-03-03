#!/usr/bin/env bash
set -euo pipefail

TOOLS_ROOT="${TOOLS_HOME:-$HOME/.tools}"
LEGACY_OBS_DIR="${OBS_HOME:-${TOOLS_ROOT}/obs}"
TAO_DIR="${TAO_HOME:-${TOOLS_ROOT}/tao}"

echo "Cleaning Rust build artifacts..."
cargo clean

echo "Cleaning Swift package artifacts..."
swift package --package-path apps/tao-macos clean

if [ -d dist ]; then
  echo "Cleaning dist artifacts..."
  find dist -mindepth 1 -depth -delete
  rmdir dist 2>/dev/null || true
fi

for candidate in "$LEGACY_OBS_DIR" "$TAO_DIR"; do
  if [ -d "$candidate" ]; then
    echo "Cleaning tool install directory: $candidate"
    find "$candidate" -mindepth 1 -depth -delete
    rmdir "$candidate" 2>/dev/null || true
  fi
done

echo "Cleanup complete."
