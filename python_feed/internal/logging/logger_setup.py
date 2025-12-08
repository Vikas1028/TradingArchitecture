"""Logging configuration for the feed service."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(level: str, file: str) -> logging.Logger:
    """Configure logging to stdout and file.

    Args:
        level: log level string (e.g., "INFO", "DEBUG").
        file: path to the log file.

    Returns:
        Configured logger instance.

    Role:
        Centralizes logging setup to mirror Go services; writes to the provided file and stdout.
    """
    log_path = Path(file).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ws_feed_service")
    logger.setLevel(level.upper())

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
