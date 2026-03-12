#!/usr/bin/env bash
# Stop python_feed_triplex launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.python_feed_triplex.service" >/dev/null 2>&1 || true
echo "python_feed_triplex launchd service unloaded."
