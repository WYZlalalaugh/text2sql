"""
MAZE runtime telemetry helpers.
"""
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import cast

_log_dir: str | None = None


def get_maze_log_path() -> str:
    """Return today's MAZE telemetry JSONL path without creating it."""
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(_get_log_dir(), f"maze_events_{date_str}.jsonl")


def record_maze_event(
    event_type: str,
    data: dict[str, object],
    workspace_id: str | None = None,
) -> None:
    """Append one structured MAZE runtime event to the daily JSONL log."""
    log_dir = _get_log_dir()
    _ = os.makedirs(log_dir, exist_ok=True)

    record = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "workspace_id": workspace_id,
        "data": _ensure_serializable(data),
    }

    with open(get_maze_log_path(), "a", encoding="utf-8") as handle:
        _ = handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def record_feature_flags(workspace_id: str | None = None) -> None:
    """Record the current MAZE feature flag snapshot."""
    try:
        from config import config
    except ImportError:
        from ..config import config

    config.refresh_feature_flags()
    record_maze_event(
        "feature_flags",
        {
            "use_workspace_context": config.use_workspace_context,
        },
        workspace_id=workspace_id,
    )


def _get_log_dir() -> str:
    if _log_dir is not None:
        return _log_dir
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def _ensure_serializable(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        normalized_mapping = cast(Mapping[object, object], value)
        return {
            str(key): _ensure_serializable(item)
            for key, item in normalized_mapping.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_ensure_serializable(item) for item in value]
    return str(value)


__all__ = [
    "get_maze_log_path",
    "record_feature_flags",
    "record_maze_event",
]
