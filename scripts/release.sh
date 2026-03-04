#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: scripts/release.sh [--target TARGET] [--out DIR]

Builds and packages Tao release artifacts from a single entrypoint.

Targets:
  all   Build CLI + macOS app packages (default)
  cli   Build/install CLI binaries and create CLI tarball
  mac   Build/sign macOS app bundle and zip

Options:
  --target TARGET   all|cli|mac (default: all)
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
  all|cli|mac)
    ;;
  *)
    echo "--target must be one of: all|cli|mac" >&2
    exit 1
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist"

release_cli() {
  local bundle_path="${DIST_DIR}/tao-cli-bundle.tar.gz"

  echo "Building release binaries..."
  cargo build --workspace --release

  echo "Installing binaries to ${OUT_DIR}..."
  mkdir -p "${OUT_DIR}"
  cp "${ROOT_DIR}/target/release/tao" "${OUT_DIR}/tao"
  cp "${ROOT_DIR}/target/release/tao-tui" "${OUT_DIR}/tao-tui"
  chmod +x "${OUT_DIR}/tao" "${OUT_DIR}/tao-tui"

  echo "Creating release bundle ${bundle_path}..."
  mkdir -p "${DIST_DIR}"
  tar -C "${OUT_DIR}" -czf "${bundle_path}" tao tao-tui

  echo "Validating installed CLI binary..."
  "${OUT_DIR}/tao" --help >/dev/null

  echo "CLI release package ready:"
  echo "  install_dir=${OUT_DIR}"
  echo "  bundle=${bundle_path}"
}

release_mac() {
  local app_name="TaoMacOSApp"
  local package_dir="${ROOT_DIR}/apps/tao-macos"
  local app_bundle="${DIST_DIR}/${app_name}.app"
  local zip_path="${DIST_DIR}/${app_name}-macos-signed.zip"
  local executable_path="${package_dir}/.build/release/${app_name}"
  local app_executable="${app_bundle}/Contents/MacOS/${app_name}"
  local bridge_dylib_source="${ROOT_DIR}/target/release/libtao_sdk_bridge.dylib"
  local app_frameworks_dir="${app_bundle}/Contents/Frameworks"
  local app_bridge_dylib="${app_frameworks_dir}/libtao_sdk_bridge.dylib"

  echo "Building Rust bridge + Swift bindings..."
  "${ROOT_DIR}/scripts/ffi.sh"

  echo "Building ${app_name} release binary..."
  swift build --configuration release --package-path "${package_dir}" --product "${app_name}"

  if [[ ! -f "${bridge_dylib_source}" ]]; then
    echo "expected bridge dylib at ${bridge_dylib_source} but it was not found" >&2
    exit 1
  fi

  echo "Assembling app bundle at ${app_bundle}..."
  rm -rf "${app_bundle}"
  mkdir -p "${app_bundle}/Contents/MacOS" "${app_bundle}/Contents/Resources" "${app_frameworks_dir}"
  cp "${executable_path}" "${app_executable}"
  cp "${bridge_dylib_source}" "${app_bridge_dylib}"
  chmod +x "${app_executable}" "${app_bridge_dylib}"

  echo "Rewriting dylib install names for self-contained app execution..."
  install_name_tool -id "@rpath/libtao_sdk_bridge.dylib" "${app_bridge_dylib}"

  local current_bridge_ref
  current_bridge_ref="$(otool -L "${app_executable}" | awk '/libtao_sdk_bridge\\.dylib/{print $1; exit}')"
  if [[ -n "${current_bridge_ref}" && "${current_bridge_ref}" != "@rpath/libtao_sdk_bridge.dylib" ]]; then
    install_name_tool -change "${current_bridge_ref}" "@rpath/libtao_sdk_bridge.dylib" "${app_executable}"
  fi
  # Rewrite common build-path variants to ensure deterministic packaging.
  install_name_tool -change "${ROOT_DIR}/target/release/libtao_sdk_bridge.dylib" "@rpath/libtao_sdk_bridge.dylib" "${app_executable}" 2>/dev/null || true
  install_name_tool -change "${ROOT_DIR}/target/release/deps/libtao_sdk_bridge.dylib" "@rpath/libtao_sdk_bridge.dylib" "${app_executable}" 2>/dev/null || true

  if ! otool -l "${app_executable}" | grep -A2 "LC_RPATH" | grep -q "@executable_path/../Frameworks"; then
    install_name_tool -add_rpath "@executable_path/../Frameworks" "${app_executable}"
  fi

  if ! otool -L "${app_executable}" | awk '{print $1}' | grep -Fxq "@rpath/libtao_sdk_bridge.dylib"; then
    echo "failed to rewrite bridge dylib linkage to @rpath for ${app_executable}" >&2
    exit 1
  fi

  cat > "${app_bundle}/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>TaoMacOSApp</string>
  <key>CFBundleIdentifier</key>
  <string>com.u29dc.tao.macos</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>CFBundleShortVersionString</key>
  <string>0.1.0</string>
  <key>CFBundleExecutable</key>
  <string>TaoMacOSApp</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>LSMinimumSystemVersion</key>
  <string>14.0</string>
</dict>
</plist>
PLIST

  echo "Signing app bundle with ad-hoc identity..."
  codesign --force --deep --sign - "${app_bundle}"

  echo "Creating signed app archive ${zip_path}..."
  mkdir -p "${DIST_DIR}"
  rm -f "${zip_path}"
  ditto -c -k --keepParent "${app_bundle}" "${zip_path}"

  echo "macOS app package ready:"
  echo "  bundle=${app_bundle}"
  echo "  archive=${zip_path}"
}

case "$TARGET" in
  all)
    release_cli
    release_mac
    ;;
  cli)
    release_cli
    ;;
  mac)
    release_mac
    ;;
esac
