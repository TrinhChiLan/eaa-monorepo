from __future__ import annotations

from pathlib import Path

import pytest

from shared.config import DEFAULT_PERSIST_EVENT_TYPES, Settings
from shared.schemas.events import EventType


def test_settings_defaults() -> None:
    settings = Settings.from_env({})
    assert settings.ring_capacity == 1000
    assert settings.persistence_queue_capacity == 10_000
    assert settings.batch_size == 50
    assert settings.flush_interval == 2.0
    assert settings.max_retries == 3
    assert settings.persist_event_types == DEFAULT_PERSIST_EVENT_TYPES
    assert settings.retention is not None
    assert settings.retention.max_rows == 1_000_000
    assert settings.retention.max_age_seconds == 7 * 86_400
    assert settings.db_path == Path("data/edge_system.db")


def test_settings_overrides_and_disabled_retention() -> None:
    settings = Settings.from_env(
        {
            "EAA_IPC_DIR": "/tmp/custom-eaa",
            "EAA_DB_PATH": "/tmp/custom.db",
            "EAA_RING_CAPACITY": "5",
            "EAA_DB_QUEUE_CAPACITY": "7",
            "EAA_DB_BATCH_SIZE": "3",
            "EAA_DB_FLUSH_INTERVAL": "0.5",
            "EAA_DB_MAX_RETRIES": "0",
            "EAA_RETENTION_MAX_ROWS": "0",
            "EAA_RETENTION_MAX_AGE_DAYS": "0",
            "EAA_PERSIST_CV_FRAMES": "1",
            "EAA_MAX_MESSAGE_BYTES": "4096",
            "EAA_LOG_LEVEL": "debug",
        }
    )
    assert settings.ipc_dir == Path("/tmp/custom-eaa")
    assert settings.db_path == Path("/tmp/custom.db")
    assert settings.ring_capacity == 5
    assert settings.persistence_queue_capacity == 7
    assert settings.batch_size == 3
    assert settings.flush_interval == 0.5
    assert settings.max_retries == 0
    assert settings.retention is None
    assert EventType.CV_FRAME in settings.persist_event_types
    assert settings.max_message_bytes == 4096
    assert settings.log_level == "DEBUG"


def test_settings_validation() -> None:
    base = {
        "ipc_dir": Path("/tmp/eaa"),
        "db_path": Path("/tmp/eaa.db"),
    }
    with pytest.raises(ValueError):
        Settings(**base, ring_capacity=0)
    with pytest.raises(ValueError):
        Settings(**base, persistence_queue_capacity=0)
    with pytest.raises(ValueError):
        Settings(**base, batch_size=0)
    with pytest.raises(ValueError):
        Settings(**base, flush_interval=0)
    with pytest.raises(ValueError):
        Settings(**base, max_retries=-1)
