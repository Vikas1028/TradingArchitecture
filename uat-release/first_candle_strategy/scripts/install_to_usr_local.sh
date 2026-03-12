#!/usr/bin/env bash
# Build and deploy first_candle_strategy into /Users/vikasbhandekar/live_services on macOS.
# Run this as your normal user; it will call sudo only if needed for live_services writes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
APP_NAME="first_candle_strategy"
APP_HOME="/Users/vikasbhandekar/live_services/${APP_NAME}"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.${APP_NAME}.service.plist"

STAGING_DIR="$(mktemp -d)"
cleanup() {
    rm -rf "${STAGING_DIR}"
}
trap cleanup EXIT

echo "Building ${APP_NAME}..."
(
    cd "${APP_DIR}"
    go build -o "${STAGING_DIR}/${APP_NAME}" ./cmd/${APP_NAME}
)

mkdir -p "${STAGING_DIR}/cmd" "${STAGING_DIR}/config" "${STAGING_DIR}/logs" "${STAGING_DIR}/scripts"
cp "${STAGING_DIR}/${APP_NAME}" "${STAGING_DIR}/cmd/${APP_NAME}"
cp "${APP_DIR}/config/first_candle_strategy_config.json" "${STAGING_DIR}/config/first_candle_strategy_config.json"
cp "${APP_DIR}/internal/scripts/first_candle_strategy.properties" "${STAGING_DIR}/scripts/first_candle_strategy.properties"
cp "${APP_DIR}/internal/scripts/first_candle_strategy.plist" "${STAGING_DIR}/scripts/first_candle_strategy.plist"
cp "${APP_DIR}/internal/scripts/start_mac.sh" "${STAGING_DIR}/scripts/start_mac.sh"
cp "${APP_DIR}/internal/scripts/stop_mac.sh" "${STAGING_DIR}/scripts/stop_mac.sh"
chmod +x "${STAGING_DIR}/scripts/start_mac.sh" "${STAGING_DIR}/scripts/stop_mac.sh"

echo "Stopping existing launch agent if loaded..."
launchctl bootout "gui/$(id -u)/com.${APP_NAME}.service" >/dev/null 2>&1 || true

echo "Installing ${APP_HOME}..."
sudo mkdir -p "${APP_HOME}"
sudo rm -rf "${APP_HOME:?}/"*
sudo cp -R "${STAGING_DIR}/." "${APP_HOME}/"
sudo mkdir -p "${APP_HOME}/logs"
sudo chown -R "$(id -un)":"$(id -gn)" "${APP_HOME}"

mkdir -p "${AGENT_DIR}"
cp "${APP_HOME}/scripts/${APP_NAME}.plist" "${PLIST_TARGET}"

echo "Installed ${APP_NAME} to ${APP_HOME}"
echo "LaunchAgent updated at ${PLIST_TARGET}"
echo "Start it with: ${APP_HOME}/scripts/start_mac.sh"
