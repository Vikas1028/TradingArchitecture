#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/strategy_signal_archive"
PLIST="${ROOT}/scripts/strategy_signal_archive.plist"
TARGET="${HOME}/Library/LaunchAgents/com.strategy_signal_archive.service.plist"
LABEL="com.strategy_signal_archive.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
mkdir -p "${ROOT}/logs"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
