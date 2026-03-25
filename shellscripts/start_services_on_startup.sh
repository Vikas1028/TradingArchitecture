#!/usr/bin/env bash
# Auto-start core trading services on user login/startup.
# Rule: run only on Monday-Friday and only before 15:30 Asia/Kolkata.

set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

TZ_VALUE="Asia/Kolkata"
DAY_OF_WEEK="$(TZ="${TZ_VALUE}" date +%u)"
CURRENT_HHMM="$(TZ="${TZ_VALUE}" date +%H%M)"
USER_ID="$(id -u)"
AGENT_DIR="${HOME}/Library/LaunchAgents"
LOG_FILE="${HOME}/Library/Logs/TradingArchitecture/startup_services.log"

mkdir -p "$(dirname "${LOG_FILE}")"

log() {
  printf '%s %s\n' "$(TZ="${TZ_VALUE}" date '+%Y-%m-%d %H:%M:%S %Z')" "$*" >> "${LOG_FILE}"
}

start_launch_agent() {
  local label="$1"
  local plist="$2"
  if [[ ! -f "${plist}" ]]; then
    log "SKIP ${label}: plist missing at ${plist}"
    return 0
  fi
  local current_pid
  current_pid="$(launchctl list | awk -v target="${label}" '$3 == target {print $1}')"
  if [[ -n "${current_pid}" && "${current_pid}" != "-" && "${current_pid}" != "0" ]]; then
    log "SKIP ${label}: already running with pid=${current_pid}"
    return 0
  fi
  launchctl bootstrap "gui/${USER_ID}" "${plist}" >/dev/null 2>&1 || true
  if launchctl kickstart -k "gui/${USER_ID}/${label}" >/dev/null 2>&1; then
    log "STARTED ${label}"
  else
    log "FAILED ${label}"
  fi
}

if (( DAY_OF_WEEK < 1 || DAY_OF_WEEK > 5 )); then
  log "SKIP all: outside Monday-Friday"
  exit 0
fi

if (( 10#${CURRENT_HHMM} > 1530 )); then
  log "SKIP all: current time ${CURRENT_HHMM} is after 15:30 ${TZ_VALUE}"
  exit 0
fi

log "START sequence accepted: weekday=${DAY_OF_WEEK} hhmm=${CURRENT_HHMM}"

# Kafka (launchd-managed in this setup)
start_launch_agent "com.kafka.service" "${AGENT_DIR}/com.kafka.service.plist"

# Monitoring (Homebrew services)
if brew services start prometheus >/dev/null 2>&1; then
  log "STARTED homebrew.mxcl.prometheus"
else
  log "FAILED homebrew.mxcl.prometheus"
fi
if brew services start grafana >/dev/null 2>&1; then
  log "STARTED homebrew.mxcl.grafana"
else
  log "FAILED homebrew.mxcl.grafana"
fi

# Trading services
start_launch_agent "com.python_feed_triplex.service" "${AGENT_DIR}/com.python_feed_triplex.service.plist"
start_launch_agent "com.marketdata.service" "${AGENT_DIR}/com.marketdata.service.plist"
start_launch_agent "com.vwap_strategy.service" "${AGENT_DIR}/com.vwap_strategy.service.plist"
start_launch_agent "com.first_candle_strategy.service" "${AGENT_DIR}/com.first_candle_strategy.service.plist"
start_launch_agent "com.ldrb_strategy.service" "${AGENT_DIR}/com.ldrb_strategy.service.plist"
start_launch_agent "com.paper_engine.service" "${AGENT_DIR}/com.paper_engine.service.plist"

log "DONE startup sequence"
