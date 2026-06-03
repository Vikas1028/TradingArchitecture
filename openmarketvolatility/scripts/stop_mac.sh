#!/usr/bin/env bash
# Stop openmarketvolatility launchd user agent on macOS.

set -euo pipefail

launchctl bootout "gui/$(id -u)/com.openmarketvolatility.service" >/dev/null 2>&1 || true
echo "openmarketvolatility launchd service unloaded."
