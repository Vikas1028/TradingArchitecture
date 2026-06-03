#!/usr/bin/env bash
# Start paper_engine as a launchd user agent on macOS.
# - Copies plist to ~/Library/LaunchAgents
# - Loads it via launchctl

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/paper_engine.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.paper_engine.service.plist"
LOCK_DIR="/tmp/paper_engine_start.lock"
APP_HOME="/Users/vikasbhandekar/live_services/paper_engine"

if [[ ! -x "${APP_HOME}/cmd/paper_engine/paper_engine" ]]; then
    echo "paper_engine binary missing at ${APP_HOME}/cmd/paper_engine/paper_engine. Build/deploy first."
    exit 1
fi

# Keep paper_engine startable at any time so dashboard/history APIs work after market hours.

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p "${APP_HOME}/logs"

if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    echo "paper_engine start is already in progress (lock: ${LOCK_DIR})."
    exit 1
fi
trap 'rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true' EXIT

launchctl bootout "gui/$(id -u)" "${PLIST_TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.paper_engine.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.paper_engine.service"
echo "paper_engine launchd service loaded from ${PLIST_TARGET}"
