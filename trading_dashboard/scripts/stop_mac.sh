#!/usr/bin/env bash
# Stop trading_dashboard launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.trading_dashboard.service" >/dev/null 2>&1 || true
echo "trading_dashboard launchd service unloaded."
