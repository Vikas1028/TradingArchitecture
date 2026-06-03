#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/vikasbhandekar/live_services/fast_movers_stratergy"
PLIST="${ROOT}/scripts/fast_movers_stratergy.plist"
TARGET="${HOME}/Library/LaunchAgents/com.fast_movers_stratergy.service.plist"
LABEL="com.fast_movers_stratergy.service"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST}" "${TARGET}"
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${TARGET}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
