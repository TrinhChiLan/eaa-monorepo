from __future__ import annotations

import json
import random
from pathlib import Path

import pytest
from pydantic import ValidationError

from mocks.feed import (
    FeedConfig,
    MockFeed,
    cv_json,
    sensor_json,
    stt_json,
    ui_json,
)
from shared.schemas.events import (
    CVFrameWire,
    IntentWire,
    SensorSnapshotWire,
    UICommandWire,
)


def test_generators_produce_valid_wire_payloads() -> None:
    rng = random.Random(7)
    timestamp = 1_700_000_000.0

    cv = CVFrameWire.model_validate(json.loads(cv_json(rng, timestamp=timestamp)))
    assert cv.timestamp == timestamp
    assert 0.0 <= cv.confidence <= 1.0

    sensor = SensorSnapshotWire.model_validate(json.loads(sensor_json(rng, timestamp=timestamp)))
    assert sensor.timestamp_ms == int(timestamp * 1000)

    intent = IntentWire.model_validate(json.loads(stt_json(rng, timestamp=timestamp)))
    assert intent.transcript
    assert intent.intent

    ui = UICommandWire.model_validate(json.loads(ui_json(rng)))
    assert ui.action


def test_feed_config_validation() -> None:
    base = {"ipc_dir": Path("/tmp/eaa")}
    with pytest.raises(ValueError):
        FeedConfig(**base, cv_rate=0)
    with pytest.raises(ValueError):
        FeedConfig(**base, sensor_rate=-1)
    with pytest.raises(ValueError):
        FeedConfig(**base, duration=-1)
    with pytest.raises(ValueError):
        FeedConfig(**base, invalid_every=-1)


def test_invalid_json_is_not_valid_wire_input() -> None:
    with pytest.raises(json.JSONDecodeError):
        json.loads(b"{this is not valid json")
    with pytest.raises(ValidationError):
        CVFrameWire.model_validate({"confidence": 2.0})


def test_mock_feed_runs_and_stops(tmp_path) -> None:
    feed = MockFeed(
        FeedConfig(
            ipc_dir=tmp_path / "ipc",
            duration=0.2,
            cv_rate=20,
            sensor_rate=10,
            stt_interval=0.05,
            ui_interval=0.05,
            invalid_every=5,
        )
    )
    feed.run()
