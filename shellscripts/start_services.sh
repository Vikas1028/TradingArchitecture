#!/bin/bash
# Start one or more launchd services for the trading apps.

set -euo pipefail

AGENT_DIR="${HOME}/Library/LaunchAgents"
APPS=("paper_engine" "marketdata" "vwap_strategy" "first_candle_strategy" "python_feed" "ldrb_strategy")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD_SCRIPT="${SCRIPT_DIR}/market_hours_guard.sh"

start_app() {
    local app="$1"
    local plist="${AGENT_DIR}/com.${app}.service.plist"
    if [[ ! -f "${plist}" ]]; then
        echo "Plist not found for ${app}: ${plist}"
        return 1
    fi
    launchctl bootout "gui/$(id -u)/com.${app}.service" >/dev/null 2>&1 || true
    launchctl bootstrap "gui/$(id -u)" "${plist}"
    launchctl kickstart -k "gui/$(id -u)/com.${app}.service"
    echo "Started ${app} (com.${app}.service)"
}

choice="${1:-}"
if [[ -z "${choice}" ]]; then
    echo "Which application to start?"
    echo "  1) paper_engine"
    echo "  2) marketdata"
    echo "  3) vwap_strategy"
    echo "  4) first_candle_strategy"
    echo "  5) python_feed"
    echo "  6) ldrb_strategy"
    echo "  7) all"
    read -r choice
fi

if [[ ! -x "${GUARD_SCRIPT}" ]]; then
    chmod +x "${GUARD_SCRIPT}"
fi

if ! "${GUARD_SCRIPT}"; then
    echo "No services were started."
    exit 0
fi

case "${choice}" in
    7|"all"|"ALL")
        for app in "${APPS[@]}"; do start_app "${app}"; done
        ;;
    1|"paper_engine") start_app "paper_engine" ;;
    2|"marketdata") start_app "marketdata" ;;
    3|"vwap_strategy") start_app "vwap_strategy" ;;
    4|"first_candle_strategy") start_app "first_candle_strategy" ;;
    5|"python_feed") start_app "python_feed" ;;
    6|"ldrb_strategy") start_app "ldrb_strategy" ;;
    *) echo "Invalid choice"; exit 1 ;;
esac
