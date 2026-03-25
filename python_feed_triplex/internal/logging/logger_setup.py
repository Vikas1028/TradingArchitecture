"""Logging configuration for the feed service."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime

from python_feed_triplex.common import constants
from python_feed_triplex.common.path import resolve_app_root, resolve_logger_config_path


@dataclass
class LoggerConfig:
    enable: bool = True
    level: str = constants.DEFAULT_LOG_LEVEL
    format: str = constants.DEFAULT_LOG_FORMAT
    log_dir: str = constants.LOG_DIR_NAME
    file_name: str = constants.DEFAULT_FILE_NAME
    console: bool = True


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter aligned with the Go services."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        millis = int(record.msecs)
        payload = {
            "level": record.levelname,
            "time": f"{timestamp}:{millis:03d}",
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def _load_logger_config() -> tuple[LoggerConfig, Path]:
    config_path = resolve_logger_config_path()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    cfg = LoggerConfig(
        enable=bool(raw.get("enable", True)),
        level=str(raw.get("level", constants.DEFAULT_LOG_LEVEL)).upper(),
        format=str(raw.get("format", constants.DEFAULT_LOG_FORMAT)).lower(),
        log_dir=str(raw.get("log_dir", constants.LOG_DIR_NAME)).strip() or constants.LOG_DIR_NAME,
        file_name=str(raw.get("file_name", constants.DEFAULT_FILE_NAME)).strip() or constants.DEFAULT_FILE_NAME,
        console=bool(raw.get("console", True)),
    )
    app_root = resolve_app_root()
    now = datetime.now()
    date_dir = app_root / cfg.log_dir / now.strftime(constants.DATE_FOLDER_LAYOUT)
    date_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{now.strftime(constants.LOG_FILE_TIME_LAYOUT)}_{cfg.file_name}"
    return cfg, date_dir / file_name


def setup_logging(level_override: str | None = None) -> logging.Logger:
    """Configure JSON logging to the per-run file and stdout."""
    cfg, log_path = _load_logger_config()
    logger = logging.getLogger("ws_feed_service")
    level_name = (level_override or cfg.level or constants.DEFAULT_LOG_LEVEL).upper()
    logger.setLevel(level_name)
    formatter: logging.Formatter
    if cfg.format == "console":
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    else:
        formatter = JsonFormatter()
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    if cfg.console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    logger.propagate = False
    logger.info(
        "logger initialized",
        extra={
            "app": constants.APP_NAME,
            "path": str(log_path),
            "configured_level": level_name,
            "configured_format": cfg.format,
        },
    )

    return logger
