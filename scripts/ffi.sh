#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT_DIR/apps/tao-macos/Sources/TaoMacOSAppScaffold/Generated"

case "$(uname -s)" in
  Darwin) LIB_EXT="dylib" ;;
  Linux) LIB_EXT="so" ;;
  *) LIB_EXT="dylib" ;;
esac

LIB_PATH="$ROOT_DIR/target/release/libtao_sdk_bridge.${LIB_EXT}"

cargo build -p tao-sdk-bridge --release
mkdir -p "$OUT_DIR"

cargo run -p tao-sdk-bridge --bin uniffi-bindgen -- \
  generate \
  --library "$LIB_PATH" \
  --language swift \
  --out-dir "$OUT_DIR"

cat > "$OUT_DIR/module.modulemap" <<'EOF'
module tao_sdk_bridgeFFI {
    header "tao_sdk_bridgeFFI.h"
    link "tao_sdk_bridge"
    export *
    use "Darwin"
    use "_Builtin_stdbool"
    use "_Builtin_stdint"
}
EOF
