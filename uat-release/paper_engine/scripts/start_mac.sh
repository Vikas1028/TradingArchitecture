#!/usr/bin/env bash
# Start paper_engine as a launchd user agent on macOS.
# - Copies plist to ~/Library/LaunchAgents
# - Loads it via launchctl

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/paper_engine.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.paper_engine.service.plist"

day_of_week="$(TZ="Asia/Kolkata" date +%u)"
current_hhmm="$(TZ="Asia/Kolkata" date +%H%M)"
if (( day_of_week < 1 || day_of_week > 5 || 10#${current_hhmm} < 830 || 10#${current_hhmm} > 1600 )); then
    echo "paper_engine not started: allowed only Monday-Friday between 08:30 and 16:00 Asia/Kolkata."
    exit 0
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p /Users/vikasbhandekar/live_services/paper_engine/logs

launchctl bootout "gui/$(id -u)/com.paper_engine.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.paper_engine.service"
echo "paper_engine launchd service loaded from ${PLIST_TARGET}"
