#!/usr/bin/env bash
set -euo pipefail

LABEL="com.real_engine_broker_sync.service"
TARGET="${HOME}/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "${TARGET}" >/dev/null 2>&1 || true
