#!/usr/bin/env bash
set -euo pipefail

TARGET="${HOME}/Library/LaunchAgents/com.s2_fallback_selector.service.plist"
LABEL="com.s2_fallback_selector.service"

launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
rm -f "${TARGET}"
