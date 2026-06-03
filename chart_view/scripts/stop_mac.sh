#!/usr/bin/env bash
set -euo pipefail

TARGET="${HOME}/Library/LaunchAgents/com.chart_view.service.plist"
LABEL="com.chart_view.service"

launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
