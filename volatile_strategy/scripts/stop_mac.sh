#!/usr/bin/env bash
# Stop volatile_strategy launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.volatile_strategy.service" >/dev/null 2>&1 || true
echo "volatile_strategy launchd service unloaded."
