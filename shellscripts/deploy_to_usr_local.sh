#!/usr/bin/env bash
# Deploy release folders from uat-release to /usr/local and install launchd plist.
# Usage: run this script from anywhere; it will prompt for which app to deploy.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RELEASE_DIR="${REPO_ROOT}/uat-release"
AGENT_DIR="${HOME}/Library/LaunchAgents"

deploy_app() {
    local app="$1"
    local src="${RELEASE_DIR}/${app}"
    local dest="/usr/local/${app}"
    local plist_target="${AGENT_DIR}/com.${app}.service.plist"

    if [[ ! -d "${src}" ]]; then
        echo "Source folder not found: ${src}"
        return 1
    fi

    echo "Deploying ${app}..."
    sudo rm -rf "${dest}"
    sudo mv "${src}" "${dest}"

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
echo "  4) python_feed"
echo "  5) all"
read -r choice

case "${choice}" in
    5|"all"| "ALL")
        deploy_app "paper_engine"
        deploy_app "marketdata"
        deploy_app "vwap_strategy"
        deploy_app "python_feed"
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
    4|"python_feed")
        deploy_app "python_feed"
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

echo "Deployment complete."
