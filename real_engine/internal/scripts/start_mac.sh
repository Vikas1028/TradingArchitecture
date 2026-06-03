#!/usr/bin/env bash
# Start real_engine as a launchd user agent on macOS.
# - Copies plist to ~/Library/LaunchAgents
# - Loads it via launchctl

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/real_engine.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.real_engine.service.plist"

# Keep real_engine startable at any time so dashboard/history APIs work after market hours.

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p /Users/vikasbhandekar/live_services/real_engine/logs

launchctl bootout "gui/$(id -u)/com.real_engine.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.real_engine.service"
echo "real_engine launchd service loaded from ${PLIST_TARGET}"
