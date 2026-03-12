#!/usr/bin/env bash
# Stop paper_engine launchd user agent on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.paper_engine.service.plist"

if [[ -f "${PLIST_TARGET}" ]]; then
    launchctl unload "${PLIST_TARGET}" || true
    echo "paper_engine launchd service unloaded."
else
    echo "No plist found at ${PLIST_TARGET}; nothing to stop."
fi
