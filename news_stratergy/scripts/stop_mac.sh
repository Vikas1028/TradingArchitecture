#!/usr/bin/env bash
# Stop news_strategy launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.news_strategy.service" >/dev/null 2>&1 || true
echo "news_strategy launchd service unloaded."
