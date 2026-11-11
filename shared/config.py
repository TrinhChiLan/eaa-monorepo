"""Environment-backed settings for the runnable demo/entry point."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from shared.constants.zmq_topics import default_ipc_dir
from shared.schemas.events import EventType
from shared.schemas.telemetry import RetentionPolicy

DEFAULT_PERSIST_EVENT_TYPES: frozenset[EventType] = frozenset(
    {EventType.SENSOR_SNAPSHOT, EventType.STT_INTENT, EventType.UI_COMMAND}
)


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    return default if raw is None or raw == "" else int(raw)


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    return default if raw is None or raw == "" else float(raw)


def _env_bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    ipc_dir: Path
    db_path: Path
    ring_capacity: int = 1000
    persistence_queue_capacity: int = 10_000
    batch_size: int = 50
    flush_interval: float = 2.0
    max_retries: int = 3
    retention: RetentionPolicy | None = None
    persist_event_types: frozenset[EventType] = DEFAULT_PERSIST_EVENT_TYPES
    max_message_bytes: int = 1_048_576
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.ring_capacity <= 0:
            raise ValueError("ring_capacity must be positive")
        if self.persistence_queue_capacity <= 0:
            raise ValueError("persistence_queue_capacity must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.flush_interval <= 0:
            raise ValueError("flush_interval must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if self.max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        values = os.environ if env is None else env

        ipc_raw = values.get("EAA_IPC_DIR")
        ipc_dir = Path(ipc_raw).expanduser() if ipc_raw else default_ipc_dir()
        db_path = Path(values.get("EAA_DB_PATH", "data/edge_system.db")).expanduser()

        max_rows_raw = _env_int(values, "EAA_RETENTION_MAX_ROWS", 1_000_000)
        max_age_days = _env_float(values, "EAA_RETENTION_MAX_AGE_DAYS", 7.0)
        retention: RetentionPolicy | None
        if max_rows_raw <= 0 and max_age_days <= 0:
            retention = None
        else:
            retention = RetentionPolicy(
                max_rows=max_rows_raw if max_rows_raw > 0 else None,
                max_age_seconds=max_age_days * 86_400 if max_age_days > 0 else None,
                prune_interval_seconds=_env_float(values, "EAA_PRUNE_INTERVAL_SECONDS", 60.0),
            )

        persist_types = set(DEFAULT_PERSIST_EVENT_TYPES)
        if _env_bool(values, "EAA_PERSIST_CV_FRAMES", False):
            persist_types.add(EventType.CV_FRAME)

        return cls(
            ipc_dir=ipc_dir,
            db_path=db_path,
            ring_capacity=_env_int(values, "EAA_RING_CAPACITY", 1000),
            persistence_queue_capacity=_env_int(values, "EAA_DB_QUEUE_CAPACITY", 10_000),
            batch_size=_env_int(values, "EAA_DB_BATCH_SIZE", 50),
            flush_interval=_env_float(values, "EAA_DB_FLUSH_INTERVAL", 2.0),
            max_retries=_env_int(values, "EAA_DB_MAX_RETRIES", 3),
            retention=retention,
            persist_event_types=frozenset(persist_types),
            max_message_bytes=_env_int(values, "EAA_MAX_MESSAGE_BYTES", 1_048_576),
            log_level=values.get("EAA_LOG_LEVEL", "INFO").upper(),
        )


__all__ = ["DEFAULT_PERSIST_EVENT_TYPES", "Settings"]
