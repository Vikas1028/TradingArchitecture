#!/usr/bin/env bash
# Stop marketdata launchd user agent on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.marketdata.service.plist"

launchctl bootout "gui/$(id -u)/com.marketdata.service" >/dev/null 2>&1 || true
echo "marketdata launchd service unloaded."
