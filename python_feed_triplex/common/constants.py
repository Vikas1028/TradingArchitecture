"""Common constants for python_feed_triplex."""

from __future__ import annotations

import csv
from pathlib import Path

APP_NAME = "python_feed_triplex"
APP_VERSION = "1.0.0"
CONFIG_DIR_NAME = "config"
CONFIG_FILE_NAME = "ws_feed_config.json"
LOGGER_CONFIG_DIR_NAME = "cmd"
LOGGER_CONFIG_FILE_NAME = "logger.json"
LOG_DIR_NAME = "logs"
DATE_FOLDER_LAYOUT = "%Y-%m-%d"
LOG_FILE_TIME_LAYOUT = "%H%M%S"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "json"
DEFAULT_FILE_NAME = "python_feed_triplex.log"
NIFTY500_FILE_NAME = "nifty500.csv"


def _resolve_nifty500_csv_path() -> Path:
    app_root = Path(__file__).resolve().parents[1]
    return app_root / CONFIG_DIR_NAME / NIFTY500_FILE_NAME


def _load_nifty500_symbol_partition() -> dict[str, int]:
    csv_path = _resolve_nifty500_csv_path()
    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return {
        str(row.get("Symbol") or "").strip().upper(): index
        for index, row in enumerate(rows)
        if str(row.get("Symbol") or "").strip()
    }


NIFTY500_SYMBOL_PARTITION = _load_nifty500_symbol_partition()
NIFTY500_COUNT = len(NIFTY500_SYMBOL_PARTITION)
