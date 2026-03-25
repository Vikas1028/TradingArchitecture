"""Path resolution helpers."""

from __future__ import annotations

from pathlib import Path

from python_feed_triplex.common import constants


def resolve_path(dir_name: str, file_name: str) -> Path:
    working_dir = Path.cwd()
    executable_dir = Path(__file__).resolve().parents[2]
    candidates = [
        working_dir / dir_name / file_name,
        working_dir / file_name,
        executable_dir / file_name,
        executable_dir / dir_name / file_name,
        executable_dir.parent / dir_name / file_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Unable to locate {file_name}")


def resolve_config_path() -> Path:
    return resolve_path(constants.CONFIG_DIR_NAME, constants.CONFIG_FILE_NAME)


def resolve_logger_config_path() -> Path:
    return resolve_path(constants.LOGGER_CONFIG_DIR_NAME, constants.LOGGER_CONFIG_FILE_NAME)


def resolve_app_root() -> Path:
    config_path = resolve_config_path()
    config_dir = config_path.parent
    if config_dir.name == constants.CONFIG_DIR_NAME:
        return config_dir.parent
    return config_dir
