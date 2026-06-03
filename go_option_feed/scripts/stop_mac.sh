#!/usr/bin/env bash
# Stop go_option_feed launchd service on macOS.

set -euo pipefail

PLIST_TARGET="${HOME}/Library/LaunchAgents/com.go_option_feed.service.plist"

launchctl bootout "gui/$(id -u)" "${PLIST_TARGET}" >/dev/null 2>&1 || launchctl remove "com.go_option_feed.service" >/dev/null 2>&1 || true
echo "go_option_feed launchd service stopped"
