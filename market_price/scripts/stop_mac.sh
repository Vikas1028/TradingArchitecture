#!/usr/bin/env bash
# Stop market_price launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.market_price.service" >/dev/null 2>&1 || true
echo "market_price launchd service unloaded."
