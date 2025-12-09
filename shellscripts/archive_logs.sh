#!/bin/bash
# Archive daily logs for all services.
# Creates logBackup/<YYYY-MM-DD>/<app>/ and moves current log files there.

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_ROOT="${BASE_DIR}/logBackup"
TODAY="$(date +%Y-%m-%d)"
DEST_DAY="${BACKUP_ROOT}/${TODAY}"

APPS=("paper_engine" "marketdata" "vwap_strategy" "python_feed")

mkdir -p "${DEST_DAY}"

move_logs() {
    local app="$1"
    local src="$2"
    local dest="${DEST_DAY}/${app}"

    if [[ ! -d "${src}" ]]; then
        echo "Skip ${app}: log dir not found (${src})"
        return
    fi

    shopt -s nullglob dotglob
    local files=("${src}"/*.log "${src}"/*.log.*)
    if (( ${#files[@]} == 0 )); then
        echo "Skip ${app}: no log files in ${src}"
        return
    fi

    mkdir -p "${dest}"
    mv "${files[@]}" "${dest}/"
    echo "Moved ${#files[@]} log file(s) for ${app} to ${dest}"
}

for app in "${APPS[@]}"; do
    move_logs "${app}" "/usr/local/${app}/logs"
done

echo "Archive complete: ${DEST_DAY}"
