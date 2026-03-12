#!/usr/bin/env bash
# Deploy release folders from uat-release to /Users/vikasbhandekar/live_services and install launchd plist.
# Usage: run this script from anywhere; it will prompt for which app to deploy.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RELEASE_DIR="${REPO_ROOT}/uat-release"
AGENT_DIR="${HOME}/Library/LaunchAgents"

deploy_app() {
    local app="$1"
    local src="${RELEASE_DIR}/${app}"
    local dest="/Users/vikasbhandekar/live_services/${app}"
    local plist_target="${AGENT_DIR}/com.${app}.service.plist"
    local service_name="com.${app}.service"

    if [[ ! -d "${src}" ]]; then
        echo "Source folder not found: ${src}"
        return 1
    fi

    echo "Deploying ${app}..."
    launchctl bootout "gui/$(id -u)/${service_name}" >/dev/null 2>&1 || true
    sudo rm -rf "${dest}"
    sudo mkdir -p "${dest}"
    sudo cp -R "${src}/." "${dest}/"

    mkdir -p "${AGENT_DIR}"
    local plist_src="${dest}/scripts/${app}.plist"
    if [[ -f "${plist_src}" ]]; then
        cp "${plist_src}" "${plist_target}"
        echo "Copied plist to ${plist_target}"
    else
        echo "Warning: plist not found for ${app} at ${plist_src}"
    fi

    # Ensure logs directory exists
    sudo mkdir -p "${dest}/logs"

    # Service is not started here; only files are placed. Use launchctl separately to start.
}

echo "Which application to deploy?"
echo "  1) paper_engine"
echo "  2) marketdata"
echo "  3) vwap_strategy"
echo "  4) first_candle_strategy"
echo "  5) python_feed"
echo "  6) ldrb_strategy"
echo "  7) all"
read -r choice

case "${choice}" in
    7|"all"| "ALL")
        deploy_app "paper_engine"
        deploy_app "marketdata"
        deploy_app "vwap_strategy"
        deploy_app "first_candle_strategy"
        deploy_app "python_feed"
        deploy_app "ldrb_strategy"
        ;;
    1|"paper_engine")
        deploy_app "paper_engine"
        ;;
    2|"marketdata")
        deploy_app "marketdata"
        ;;
    3|"vwap_strategy")
        deploy_app "vwap_strategy"
        ;;
    4|"first_candle_strategy")
        deploy_app "first_candle_strategy"
        ;;
    5|"python_feed")
        deploy_app "python_feed"
        ;;
    6|"ldrb_strategy")
        deploy_app "ldrb_strategy"
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo "Deployment complete."
