#!/usr/bin/env bash
# Start go_option_feed as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/go_option_feed.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.go_option_feed.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/go_option_feed"

if [[ ! -x "${APP_HOME}/cmd/go_option_feed" ]]; then
    echo "go_option_feed is not deployed at ${APP_HOME}. Deploy it before starting."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"
mkdir -p "${APP_HOME}/logs"

launchctl bootout "gui/$(id -u)/com.go_option_feed.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.go_option_feed.service"
echo "go_option_feed launchd service loaded from ${PLIST_TARGET}"
