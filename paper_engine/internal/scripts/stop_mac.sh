#!/usr/bin/env bash
# Stop paper_engine launchd user agent on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.paper_engine.service.plist"
LOCK_DIR="/tmp/paper_engine_start.lock"

if [[ -f "${PLIST_TARGET}" ]]; then
    launchctl unload "${PLIST_TARGET}" || true
    echo "paper_engine launchd service unloaded."
else
    echo "No plist found at ${PLIST_TARGET}; nothing to stop."
fi

rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true
