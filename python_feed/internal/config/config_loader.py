"""Config loading and validation utilities."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "ws_feed_config.json"


# load_config reads the JSON config, applies defaults, validates required fields, and ensures log dir exists.
# Parameters:
# - path: path to the JSON config file.
# Returns:
# - parsed configuration dictionary with defaults applied.
# Flow: read JSON, fill defaults, warn on missing non-critical fields, raise on missing critical fields, ensure log directory.
def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    logger = logging.getLogger("ws_feed_service")
    cfg_path = Path(path).expanduser()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    try:
        cfg: Dict[str, Any] = json.loads(cfg_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in config: {exc}") from exc

    cfg.setdefault("angel", {})
    cfg.setdefault("kafka", {})
    cfg.setdefault("symbols", {"indices": [], "equities": []})
    cfg.setdefault("token_map", {})
    cfg.setdefault("log", {"level": "INFO", "file": "logs/ws_feed_service.log"})
    cfg.setdefault("reconnect", {"max_retries": 0, "backoff_seconds": 5})
    cfg.setdefault("health", {"print_stats_interval_sec": 60})

    required: List[Tuple[str, str]] = [
        ("angel", "api_key"),
        ("angel", "client_id"),
        ("kafka", "bootstrap_servers"),
        ("kafka", "topic"),
    ]
    missing = [(section, key) for section, key in required if not cfg.get(section, {}).get(key)]
    if missing:
        missing_str = ", ".join(f"{sec}.{key}" for sec, key in missing)
        raise ValueError(f"Missing required config fields: {missing_str}")

    # Warn on optional gaps.
    optional_defaults = {
        ("angel", "refresh_token"): None,
        ("angel", "feed_token"): cfg["angel"].get("refresh_token"),
        ("angel", "password"): None,
        ("angel", "totp_secret"): None,
        ("kafka", "acks"): "1",
        ("kafka", "linger_ms"): 5,
        ("kafka", "batch_size"): 32768,
        ("log", "level"): "INFO",
        ("log", "file"): "logs/ws_feed_service.log",
        ("reconnect", "max_retries"): 0,
        ("reconnect", "backoff_seconds"): 5,
        ("health", "print_stats_interval_sec"): 60,
    }
    for (section, key), default in optional_defaults.items():
        section_dict = cfg.setdefault(section, {})
        if key not in section_dict or section_dict[key] is None:
            logger.warning("Config missing %s.%s, using default", section, key)
            section_dict[key] = default

    log_path = Path(cfg["log"]["file"]).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cfg["log"]["file"] = str(log_path)

    return cfg
