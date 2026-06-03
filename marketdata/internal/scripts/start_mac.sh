#!/usr/bin/env bash
# Start marketdata as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/marketdata.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.marketdata.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/marketdata"

if [[ ! -x "${APP_HOME}/cmd/marketdata/marketdata" ]]; then
    echo "marketdata binary missing at ${APP_HOME}/cmd/marketdata/marketdata. Build/deploy first."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p "${APP_HOME}/logs"

launchctl bootout "gui/$(id -u)" "${PLIST_TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.marketdata.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.marketdata.service"
echo "marketdata launchd service loaded from ${PLIST_TARGET}"
