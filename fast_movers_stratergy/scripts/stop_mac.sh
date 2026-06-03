#!/usr/bin/env bash
set -euo pipefail

TARGET="${HOME}/Library/LaunchAgents/com.fast_movers_stratergy.service.plist"
LABEL="com.fast_movers_stratergy.service"

launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
