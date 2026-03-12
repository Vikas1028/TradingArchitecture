#!/usr/bin/env bash
# Allow trading service starts only on weekdays between 08:30 and 16:00 Asia/Kolkata.

set -euo pipefail

TZ_VALUE="Asia/Kolkata"
day_of_week="$(TZ="${TZ_VALUE}" date +%u)"
current_hhmm="$(TZ="${TZ_VALUE}" date +%H%M)"

if (( day_of_week < 1 || day_of_week > 5 )); then
    echo "Outside trading days. Services can start only Monday to Friday."
    exit 1
fi

if (( 10#${current_hhmm} < 830 || 10#${current_hhmm} > 1600 )); then
    echo "Outside trading hours. Services can start only between 08:30 and 16:00 Asia/Kolkata."
    exit 1
fi

echo "Within trading window (${TZ_VALUE}); service start allowed."
