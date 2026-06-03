#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/s2_fallback_selector"
PLIST="${ROOT}/scripts/s2_fallback_selector.plist"
TARGET="${HOME}/Library/LaunchAgents/com.s2_fallback_selector.service.plist"
LABEL="com.s2_fallback_selector.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
mkdir -p "${ROOT}/logs"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
