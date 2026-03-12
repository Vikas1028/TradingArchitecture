#!/usr/bin/env bash
# Start python_feed_triplex as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/python_feed.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.python_feed_triplex.service.plist"

day_of_week="$(TZ="Asia/Kolkata" date +%u)"
current_hhmm="$(TZ="Asia/Kolkata" date +%H%M)"
if (( day_of_week < 1 || day_of_week > 5 || 10#${current_hhmm} < 830 || 10#${current_hhmm} > 1600 )); then
    echo "python_feed_triplex not started: allowed only Monday-Friday between 08:30 and 16:00 Asia/Kolkata."
    exit 0
fi

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p /Users/vikasbhandekar/live_services/python_feed_triplex/logs

launchctl bootout "gui/$(id -u)/com.python_feed_triplex.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.python_feed_triplex.service"
echo "python_feed_triplex launchd service loaded from ${PLIST_TARGET}"
