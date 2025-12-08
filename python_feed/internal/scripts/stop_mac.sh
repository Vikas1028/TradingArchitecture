#!/usr/bin/env bash
# Stop python_feed launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.python_feed.service" >/dev/null 2>&1 || true
echo "python_feed launchd service unloaded."
