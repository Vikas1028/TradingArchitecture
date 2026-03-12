#!/usr/bin/env bash
# Start ldrb_strategy as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/ldrb_strategy.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.ldrb_strategy.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/ldrb_strategy"

day_of_week="$(TZ="Asia/Kolkata" date +%u)"
current_hhmm="$(TZ="Asia/Kolkata" date +%H%M)"
if (( day_of_week < 1 || day_of_week > 5 || 10#${current_hhmm} < 830 || 10#${current_hhmm} > 1600 )); then
    echo "ldrb_strategy not started: allowed only Monday-Friday between 08:30 and 16:00 Asia/Kolkata."
    exit 0
fi

if [[ ! -x "${APP_HOME}/cmd/ldrb_strategy" ]]; then
    echo "ldrb_strategy is not deployed at ${APP_HOME}. Deploy it before starting."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"
mkdir -p "${APP_HOME}/logs"

launchctl bootout "gui/$(id -u)/com.ldrb_strategy.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.ldrb_strategy.service"
echo "ldrb_strategy launchd service loaded from ${PLIST_TARGET}"

