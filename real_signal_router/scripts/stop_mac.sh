#!/usr/bin/env bash
set -euo pipefail

TARGET="${HOME}/Library/LaunchAgents/com.real_signal_router.service.plist"
LABEL="com.real_signal_router.service"

launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
