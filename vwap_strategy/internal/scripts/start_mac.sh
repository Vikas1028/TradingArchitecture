#!/usr/bin/env bash
# Start vwap_strategy as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/vwap_strategy.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.vwap_strategy.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/vwap_strategy"

if [[ ! -x "${APP_HOME}/vwap_strategy" ]]; then
    echo "vwap_strategy binary missing at ${APP_HOME}/vwap_strategy. Build/deploy first."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p "${APP_HOME}/logs"

launchctl bootout "gui/$(id -u)" "${PLIST_TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.vwap_strategy.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.vwap_strategy.service"
echo "vwap_strategy launchd service loaded from ${PLIST_TARGET}"
