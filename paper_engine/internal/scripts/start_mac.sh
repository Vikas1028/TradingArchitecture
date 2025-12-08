#!/usr/bin/env bash
# Start paper_engine as a launchd user agent on macOS.
# - Copies plist to ~/Library/LaunchAgents
# - Loads it via launchctl

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/paper_engine.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.paper_engine.service.plist"

mkdir -p "${AGENT_DIR}"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

# Ensure log directory exists
mkdir -p /usr/local/paper_engine/logs

launchctl unload "${PLIST_TARGET}" >/dev/null 2>&1 || true
launchctl load -w "${PLIST_TARGET}"
echo "paper_engine launchd service loaded from ${PLIST_TARGET}"
