#!/usr/bin/env bash
# Start go_ltp as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/go_ltp.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.go_ltp.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/go_ltp"

if [[ ! -x "${APP_HOME}/cmd/go_ltp" ]]; then
    echo "go_ltp is not deployed at ${APP_HOME}. Deploy it before starting."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"
mkdir -p "${APP_HOME}/logs"

launchctl bootout "gui/$(id -u)/com.go_ltp.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.go_ltp.service"
echo "go_ltp launchd service loaded from ${PLIST_TARGET}"
