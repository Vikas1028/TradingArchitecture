#!/usr/bin/env bash
# Start python_feed as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/python_feed.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.python_feed.service.plist"

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p /usr/local/python_feed/logs

launchctl bootout "gui/$(id -u)/com.python_feed.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
echo "python_feed launchd service loaded from ${PLIST_TARGET}"
