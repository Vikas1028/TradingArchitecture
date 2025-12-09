#!/bin/bash
# Start one or more launchd services for the trading apps.

set -euo pipefail

AGENT_DIR="${HOME}/Library/LaunchAgents"
APPS=("paper_engine" "marketdata" "vwap_strategy" "python_feed")

start_app() {
    local app="$1"
    local plist="${AGENT_DIR}/com.${app}.service.plist"
    if [[ ! -f "${plist}" ]]; then
        echo "Plist not found for ${app}: ${plist}"
        return 1
    fi
    launchctl bootstrap "gui/$(id -u)" "${plist}"
    echo "Started ${app} (com.${app}.service)"
}

echo "Which application to start?"
echo "  1) paper_engine"
echo "  2) marketdata"
echo "  3) vwap_strategy"
echo "  4) python_feed"
echo "  5) all"
read -r choice

case "${choice}" in
    5|"all"|"ALL")
        for app in "${APPS[@]}"; do start_app "${app}"; done
        ;;
    1|"paper_engine") start_app "paper_engine" ;;
    2|"marketdata") start_app "marketdata" ;;
    3|"vwap_strategy") start_app "vwap_strategy" ;;
    4|"python_feed") start_app "python_feed" ;;
    *) echo "Invalid choice"; exit 1 ;;
esac
