#!/bin/bash
# Archive daily logs for trading services into the repo-level logs folder.
# Creates logs/<YYYY-MM-DD>/<service>/ and moves current log files there.

set -euo pipefail

ARCHIVE_ROOT="${ARCHIVE_ROOT:-/Users/vikasbhandekar/Desktop/TradingArchitecture/logs}"
TODAY="$(date +%Y-%m-%d)"
DEST_DAY="${ARCHIVE_ROOT}/${TODAY}"

mkdir -p "${DEST_DAY}"

move_matching_logs() {
    local service="$1"
    local src_dir="$2"
    shift 2
    local patterns=("$@")
    local dest="${DEST_DAY}/${service}"

    if [[ ! -d "${src_dir}" ]]; then
        echo "Skip ${service}: log dir not found (${src_dir})"
        return
    fi

    mkdir -p "${dest}"

    local moved=0
    local pattern
    local file
    for pattern in "${patterns[@]}"; do
        shopt -s nullglob
        for file in "${src_dir}"/${pattern}; do
            if [[ ! -f "${file}" ]]; then
                continue
            fi
            mv "${file}" "${dest}/"
            touch "${file}"
            moved=$((moved + 1))
        done
        shopt -u nullglob
    done

    if (( moved == 0 )); then
        echo "Skip ${service}: no matching log files in ${src_dir}"
        return
    fi

    echo "Moved ${moved} log file(s) for ${service} to ${dest}"
}

FIRST_CANDLE_LOG_DIR="/Users/vikasbhandekar/live_services/first_candle_strategy/logs"
if [[ ! -d "${FIRST_CANDLE_LOG_DIR}" ]]; then
    FIRST_CANDLE_LOG_DIR="/Users/vikasbhandekar/Desktop/TradingArchitecture/uat-release/first_candle_strategy/logs"
fi

LDRB_LOG_DIR_PRIMARY="/Users/vikasbhandekar/live_services/ldrb_strategy/logs"
LDRB_LOG_DIR_SECONDARY="/Users/vikasbhandekar/Desktop/TradingArchitecture/ldrb_strategy/logs"

move_matching_logs "python_feed" "/Users/vikasbhandekar/live_services/python_feed/logs" "*.log" "*.log.*" "service.log"
move_matching_logs "marketdata" "/Users/vikasbhandekar/live_services/marketdata/logs" "*.log" "*.log.*"
move_matching_logs "vwap_strategy" "/Users/vikasbhandekar/live_services/vwap_strategy/logs" "*.log" "*.log.*"
move_matching_logs "first_candle_strategy" "${FIRST_CANDLE_LOG_DIR}" "*.log" "*.log.*"
move_matching_logs "ldrb_strategy" "${LDRB_LOG_DIR_PRIMARY}" "*.log" "*.log.*"
if [[ "${LDRB_LOG_DIR_SECONDARY}" != "${LDRB_LOG_DIR_PRIMARY}" ]]; then
    move_matching_logs "ldrb_strategy" "${LDRB_LOG_DIR_SECONDARY}" "*.log" "*.log.*"
fi
move_matching_logs "paper_engine" "/Users/vikasbhandekar/live_services/paper_engine/logs" "*.log" "*.log.*"
move_matching_logs "prometheus" "/opt/homebrew/var/log" "prometheus.log" "prometheus.err.log"
move_matching_logs "grafana" "/opt/homebrew/var/log" "grafana-stdout.log" "grafana-stderr.log"

echo "Archive complete: ${DEST_DAY}"
