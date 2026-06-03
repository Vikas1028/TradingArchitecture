#!/usr/bin/env bash
# Start news_strategy as a launchd user agent on macOS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLIST_SOURCE="${SCRIPT_DIR}/news_strategy.plist"
AGENT_DIR="${HOME}/Library/LaunchAgents"
PLIST_TARGET="${AGENT_DIR}/com.news_strategy.service.plist"
APP_HOME="/Users/vikasbhandekar/live_services/news_stratergy"

if [[ ! -x "${APP_HOME}/cmd/news_strategy" ]]; then
    echo "news_strategy is not deployed at ${APP_HOME}. Deploy it before starting."
    exit 1
fi

mkdir -p "${AGENT_DIR}"
mkdir -p "${APP_HOME}/logs"
cp "${PLIST_SOURCE}" "${PLIST_TARGET}"

launchctl bootout "gui/$(id -u)/com.news_strategy.service" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${PLIST_TARGET}"
launchctl kickstart -k "gui/$(id -u)/com.news_strategy.service"
echo "news_strategy launchd service loaded from ${PLIST_TARGET}"
