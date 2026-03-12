#!/usr/bin/env bash
# Stop ldrb_strategy launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.ldrb_strategy.service" >/dev/null 2>&1 || true
echo "ldrb_strategy launchd service unloaded."

