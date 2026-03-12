#!/bin/bash
# Stop one or more launchd services for the trading apps.

set -euo pipefail

APPS=("paper_engine" "marketdata" "vwap_strategy" "first_candle_strategy" "python_feed" "ldrb_strategy")

stop_app() {
    local app="$1"
    launchctl bootout "gui/$(id -u)/com.${app}.service" >/dev/null 2>&1 || true
    echo "Stopped ${app} (com.${app}.service)"
}

echo "Which application to stop?"
echo "  1) paper_engine"
echo "  2) marketdata"
echo "  3) vwap_strategy"
echo "  4) first_candle_strategy"
echo "  5) python_feed"
echo "  6) ldrb_strategy"
echo "  7) all"
read -r choice

case "${choice}" in
    7|"all"|"ALL")
        for app in "${APPS[@]}"; do stop_app "${app}"; done
        ;;
    1|"paper_engine") stop_app "paper_engine" ;;
    2|"marketdata") stop_app "marketdata" ;;
    3|"vwap_strategy") stop_app "vwap_strategy" ;;
    4|"first_candle_strategy") stop_app "first_candle_strategy" ;;
    5|"python_feed") stop_app "python_feed" ;;
    6|"ldrb_strategy") stop_app "ldrb_strategy" ;;
    *) echo "Invalid choice"; exit 1 ;;
esac
