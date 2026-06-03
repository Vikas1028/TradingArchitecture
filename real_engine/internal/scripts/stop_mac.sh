#!/usr/bin/env bash
# Stop real_engine launchd user agent on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.real_engine.service.plist"

if [[ -f "${PLIST_TARGET}" ]]; then
    launchctl unload "${PLIST_TARGET}" || true
    echo "real_engine launchd service unloaded."
else
    echo "No plist found at ${PLIST_TARGET}; nothing to stop."
fi
