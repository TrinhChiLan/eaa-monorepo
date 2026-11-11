from __future__ import annotations

import math

import pytest
from pydantic import ValidationError

from shared.schemas.events import (
    CVFramePayload,
    CVFrameWire,
    EventEnvelope,
    EventSource,
    EventType,
    IntentPayload,
    SensorSnapshotPayload,
    UICommandPayload,
)
from tests.support import make_sensor_event


def test_envelope_json_roundtrip() -> None:
    event = make_sensor_event(event_time=1_700_000_000.0)
    restored = EventEnvelope.model_validate_json(event.model_dump_json())
    assert restored == event
    assert restored.event_type is EventType.SENSOR_SNAPSHOT
    assert restored.source is EventSource.SENSOR
    assert restored.payload.kind == "sensor.snapshot"


def test_event_type_must_match_payload_kind() -> None:
    with pytest.raises(ValidationError, match="does not match"):
        EventEnvelope(
            event_id="abc",
            event_type=EventType.CV_FRAME,
            source=EventSource.CV,
            topic="cv.frame",
            sequence=0,
            event_time=1_700_000_000.0,
            ingested_at=1_700_000_000.0,
            payload=IntentPayload(transcript="x", intent="y", confidence=0.5),
        )


def test_confidence_bounds_are_enforced() -> None:
    with pytest.raises(ValidationError):
        CVFramePayload(confidence=1.5)
    with pytest.raises(ValidationError):
        IntentPayload(transcript="x", intent="y", confidence=-0.1)


def test_frozen_envelope_rejects_mutation() -> None:
    event = make_sensor_event()
    with pytest.raises(ValidationError):
        event.sequence = 99  # type: ignore[misc]


def test_canonical_envelope_rejects_unknown_fields() -> None:
    event = make_sensor_event()
    data = event.model_dump(mode="json")
    data["unexpected"] = True
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(data)


def test_wire_model_ignores_unknown_fields() -> None:
    wire = CVFrameWire.model_validate(
        {
            "confidence": 0.8,
            "detected_objects": ["carton"],
            "future_field": "ignored",
        }
    )
    assert wire.confidence == 0.8
    assert wire.camera_id == "cam0"


def test_non_finite_and_non_positive_times_are_rejected() -> None:
    with pytest.raises(ValidationError):
        CVFrameWire(timestamp=math.nan, confidence=0.5)
    with pytest.raises(ValidationError):
        EventEnvelope.create(
            payload=SensorSnapshotPayload(
                motor_current_amp=1.0,
                infrared_temp_celsius=40.0,
                conveyor_speed_mms=100.0,
                optical_curtain_clear=True,
                estop_circuit_closed=True,
            ),
            source=EventSource.SENSOR,
            topic="sensor.snapshot",
            sequence=0,
            event_time=-1.0,
        )


def test_unknown_payload_kind_is_rejected() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope.model_validate(
            {
                "event_id": "abc",
                "event_type": "sensor.snapshot",
                "source": "sensor",
                "topic": "sensor.snapshot",
                "sequence": 0,
                "event_time": 1_700_000_000.0,
                "ingested_at": 1_700_000_000.0,
                "payload": {"kind": "not.a.kind"},
            }
        )


def test_ui_parameters_reject_non_finite_numbers() -> None:
    with pytest.raises(ValidationError):
        UICommandPayload(action="pause", parameters={"threshold": math.nan})
