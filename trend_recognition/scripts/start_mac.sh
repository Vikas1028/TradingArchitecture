#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/trend_recognition"
PLIST="${ROOT}/scripts/trend_recognition.plist"
TARGET="${HOME}/Library/LaunchAgents/com.trend_recognition.service.plist"
LABEL="com.trend_recognition.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
