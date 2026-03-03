#!/usr/bin/env bash
set -euo pipefail

APP_NAME="ObsMacOSApp"
PACKAGE_DIR="apps/obs-macos"
DIST_DIR="dist"
APP_BUNDLE="${DIST_DIR}/${APP_NAME}.app"
ZIP_PATH="${DIST_DIR}/${APP_NAME}-macos-signed.zip"
EXECUTABLE_PATH="${PACKAGE_DIR}/.build/release/${APP_NAME}"

echo "Building ${APP_NAME} release binary..."
swift build --configuration release --package-path "${PACKAGE_DIR}" --product "${APP_NAME}"

echo "Assembling app bundle at ${APP_BUNDLE}..."
rm -rf "${APP_BUNDLE}"
mkdir -p "${APP_BUNDLE}/Contents/MacOS" "${APP_BUNDLE}/Contents/Resources"
cp "${EXECUTABLE_PATH}" "${APP_BUNDLE}/Contents/MacOS/${APP_NAME}"
chmod +x "${APP_BUNDLE}/Contents/MacOS/${APP_NAME}"

cat > "${APP_BUNDLE}/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key>
  <string>ObsMacOSApp</string>
  <key>CFBundleIdentifier</key>
  <string>com.u29dc.obs.macos</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>CFBundleShortVersionString</key>
  <string>0.1.0</string>
  <key>CFBundleExecutable</key>
  <string>ObsMacOSApp</string>
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
