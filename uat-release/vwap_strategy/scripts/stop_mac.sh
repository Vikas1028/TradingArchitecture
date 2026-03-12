#!/usr/bin/env bash
# Stop vwap_strategy launchd user agent on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.vwap_strategy.service.plist"

launchctl bootout "gui/$(id -u)/com.vwap_strategy.service" >/dev/null 2>&1 || true
echo "vwap_strategy launchd service unloaded."
