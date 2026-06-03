#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/real_signal_router"
PLIST="${ROOT}/scripts/real_signal_router.plist"
TARGET="${HOME}/Library/LaunchAgents/com.real_signal_router.service.plist"
LABEL="com.real_signal_router.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
mkdir -p "${ROOT}/logs"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
