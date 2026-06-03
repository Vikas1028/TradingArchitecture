#!/usr/bin/env bash
# Start market_price as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/market_price.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.market_price.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/market_price"

if [[ ! -x "${APP_HOME}/cmd/market_price" ]]; then
    echo "market_price is not deployed at ${APP_HOME}. Deploy it before starting."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
mkdir -p "${APP_HOME}/logs"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

launchctl bootout "gui/$(id -u)/com.market_price.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.market_price.service"
echo "market_price launchd service loaded from ${PLIST_TARGET}"
