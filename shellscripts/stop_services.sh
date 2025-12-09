#!/bin/bash
# Stop one or more launchd services for the trading apps.

set -euo pipefail

APPS=("paper_engine" "marketdata" "vwap_strategy" "python_feed")

stop_app() {
    local app="$1"
    launchctl bootout "gui/$(id -u)/com.${app}.service" >/dev/null 2>&1 || true
    echo "Stopped ${app} (com.${app}.service)"
}

echo "Which application to stop?"
echo "  1) paper_engine"
echo "  2) marketdata"
echo "  3) vwap_strategy"
echo "  4) python_feed"
echo "  5) all"
read -r choice

case "${choice}" in
    5|"all"|"ALL")
        for app in "${APPS[@]}"; do stop_app "${app}"; done
        ;;
    1|"paper_engine") stop_app "paper_engine" ;;
    2|"marketdata") stop_app "marketdata" ;;
    3|"vwap_strategy") stop_app "vwap_strategy" ;;
    4|"python_feed") stop_app "python_feed" ;;
    *) echo "Invalid choice"; exit 1 ;;
esac
