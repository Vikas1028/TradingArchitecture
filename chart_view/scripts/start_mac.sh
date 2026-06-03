#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/chart_view"
PLIST="${ROOT}/scripts/chart_view.plist"
TARGET="${HOME}/Library/LaunchAgents/com.chart_view.service.plist"
LABEL="com.chart_view.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
