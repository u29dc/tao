#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="TaoMacOSApp"
PACKAGE_DIR="${ROOT_DIR}/apps/tao-macos"
DIST_DIR="${ROOT_DIR}/dist"
APP_BUNDLE="${DIST_DIR}/${APP_NAME}.app"
ZIP_PATH="${DIST_DIR}/${APP_NAME}-macos-signed.zip"
EXECUTABLE_PATH="${PACKAGE_DIR}/.build/release/${APP_NAME}"
APP_EXECUTABLE="${APP_BUNDLE}/Contents/MacOS/${APP_NAME}"
BRIDGE_DYLIB_SOURCE="${ROOT_DIR}/target/release/libtao_sdk_bridge.dylib"
APP_FRAMEWORKS_DIR="${APP_BUNDLE}/Contents/Frameworks"
APP_BRIDGE_DYLIB="${APP_FRAMEWORKS_DIR}/libtao_sdk_bridge.dylib"

echo "Building Rust bridge + Swift bindings..."
"${ROOT_DIR}/scripts/ffi.sh"

echo "Building ${APP_NAME} release binary..."
swift build --configuration release --package-path "${PACKAGE_DIR}" --product "${APP_NAME}"

if [[ ! -f "${BRIDGE_DYLIB_SOURCE}" ]]; then
  echo "expected bridge dylib at ${BRIDGE_DYLIB_SOURCE} but it was not found" >&2
  exit 1
fi

echo "Assembling app bundle at ${APP_BUNDLE}..."
rm -rf "${APP_BUNDLE}"
mkdir -p "${APP_BUNDLE}/Contents/MacOS" "${APP_BUNDLE}/Contents/Resources" "${APP_FRAMEWORKS_DIR}"
cp "${EXECUTABLE_PATH}" "${APP_EXECUTABLE}"
cp "${BRIDGE_DYLIB_SOURCE}" "${APP_BRIDGE_DYLIB}"
chmod +x "${APP_EXECUTABLE}" "${APP_BRIDGE_DYLIB}"

echo "Rewriting dylib install names for self-contained app execution..."
install_name_tool -id "@rpath/libtao_sdk_bridge.dylib" "${APP_BRIDGE_DYLIB}"

CURRENT_BRIDGE_REF="$(otool -L "${APP_EXECUTABLE}" | awk '/libtao_sdk_bridge\\.dylib/{print $1; exit}')"
if [[ -n "${CURRENT_BRIDGE_REF}" && "${CURRENT_BRIDGE_REF}" != "@rpath/libtao_sdk_bridge.dylib" ]]; then
  install_name_tool -change "${CURRENT_BRIDGE_REF}" "@rpath/libtao_sdk_bridge.dylib" "${APP_EXECUTABLE}"
fi
# Rewrite common build-path variants to ensure deterministic packaging.
install_name_tool -change "${ROOT_DIR}/target/release/libtao_sdk_bridge.dylib" "@rpath/libtao_sdk_bridge.dylib" "${APP_EXECUTABLE}" 2>/dev/null || true
install_name_tool -change "${ROOT_DIR}/target/release/deps/libtao_sdk_bridge.dylib" "@rpath/libtao_sdk_bridge.dylib" "${APP_EXECUTABLE}" 2>/dev/null || true

if ! otool -l "${APP_EXECUTABLE}" | grep -A2 "LC_RPATH" | grep -q "@executable_path/../Frameworks"; then
  install_name_tool -add_rpath "@executable_path/../Frameworks" "${APP_EXECUTABLE}"
fi

if ! otool -L "${APP_EXECUTABLE}" | awk '{print $1}' | grep -Fxq "@rpath/libtao_sdk_bridge.dylib"; then
  echo "failed to rewrite bridge dylib linkage to @rpath for ${APP_EXECUTABLE}" >&2
  exit 1
fi

cat > "${APP_BUNDLE}/Contents/Info.plist" <<'PLIST'
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
codesign --force --deep --sign - "${APP_BUNDLE}"

echo "Creating signed app archive ${ZIP_PATH}..."
mkdir -p "${DIST_DIR}"
rm -f "${ZIP_PATH}"
ditto -c -k --keepParent "${APP_BUNDLE}" "${ZIP_PATH}"

echo "macOS app package ready:"
echo "  bundle=${APP_BUNDLE}"
echo "  archive=${ZIP_PATH}"
