#!/usr/bin/env bash
# Stop first_candle_strategy launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.first_candle_strategy.service" >/dev/null 2>&1 || true
echo "first_candle_strategy launchd service unloaded."
