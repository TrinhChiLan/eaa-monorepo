"""Canonical internal event contract shared by every module.

All data crossing the router -> ring buffer -> persistence boundary is an
:class:`EventEnvelope`.  External producers use the ``*Wire`` models below; the
router validates those and converts them into a canonical payload.
"""

from __future__ import annotations

import math
import time
import uuid
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = 1

# A JSON-representable scalar.  UI commands are deliberately flat in this slice.
JsonScalar = str | int | float | bool | None


class EventType(StrEnum):
    """Canonical event types. Values are also the Pydantic payload discriminators."""

    CV_FRAME = "cv.frame"
    STT_INTENT = "stt.intent"
    SENSOR_SNAPSHOT = "sensor.snapshot"
    UI_COMMAND = "ui.command"


class EventSource(StrEnum):
    CV = "cv"
    STT = "stt"
    SENSOR = "sensor"
    UI = "ui"
    SYSTEM = "system"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CVFramePayload(_FrozenModel):
    kind: Literal["cv.frame"] = EventType.CV_FRAME.value
    camera_id: str = Field(default="cam0", min_length=1, max_length=64)
    detected_objects: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    jam_suspected: bool = False


class IntentPayload(_FrozenModel):
    kind: Literal["stt.intent"] = EventType.STT_INTENT.value
    transcript: str = Field(min_length=1, max_length=4096)
    intent: str = Field(min_length=1, max_length=128)
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)


class SensorSnapshotPayload(_FrozenModel):
    kind: Literal["sensor.snapshot"] = EventType.SENSOR_SNAPSHOT.value
    motor_current_amp: float = Field(ge=0.0, allow_inf_nan=False)
    infrared_temp_celsius: float = Field(allow_inf_nan=False)
    conveyor_speed_mms: float = Field(allow_inf_nan=False)
    optical_curtain_clear: bool
    estop_circuit_closed: bool
    sensor_timestamp_ms: int | None = Field(default=None, ge=0)


class UICommandPayload(_FrozenModel):
    kind: Literal["ui.command"] = EventType.UI_COMMAND.value
    action: str = Field(min_length=1, max_length=128)
    parameters: dict[str, JsonScalar] = Field(default_factory=dict)

    @field_validator("parameters")
    @classmethod
    def _parameters_must_be_finite(cls, value: dict[str, JsonScalar]) -> dict[str, JsonScalar]:
        for key, item in value.items():
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError(f"parameter {key!r} must be a finite number")
        return value


EventPayload = Annotated[
    CVFramePayload | IntentPayload | SensorSnapshotPayload | UICommandPayload,
    Field(discriminator="kind"),
]


class EventEnvelope(_FrozenModel):
    """The only payload shape that may cross module boundaries."""

    schema_version: int = Field(default=SCHEMA_VERSION, ge=1)
    event_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    source: EventSource
    topic: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=0)
    event_time: float = Field(gt=0.0, allow_inf_nan=False)
    ingested_at: float = Field(gt=0.0, allow_inf_nan=False)
    payload: EventPayload

    @model_validator(mode="after")
    def _event_type_matches_payload(self) -> EventEnvelope:
        if self.event_type.value != self.payload.kind:
            raise ValueError(
                f"event_type {self.event_type.value!r} does not match "
                f"payload.kind {self.payload.kind!r}"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        payload: EventPayload,
        source: EventSource,
        topic: str,
        sequence: int,
        event_time: float | None = None,
        ingested_at: float | None = None,
        event_id: str | None = None,
    ) -> EventEnvelope:
        """Build an envelope, filling id and timestamps when the caller omits them."""
        now = time.time()
        return cls(
            schema_version=SCHEMA_VERSION,
            event_id=event_id or uuid.uuid4().hex,
            event_type=EventType(payload.kind),
            source=source,
            topic=topic,
            sequence=sequence,
            event_time=now if event_time is None else event_time,
            ingested_at=now if ingested_at is None else ingested_at,
            payload=payload,
        )


# ---------------------------------------------------------------------------
# External wire contracts.  These deliberately ignore unknown fields so a
# newer producer does not break an older edge device.
# ---------------------------------------------------------------------------


class _WireModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CVFrameWire(_WireModel):
    timestamp: float | None = Field(default=None, gt=0.0, allow_inf_nan=False)
    camera_id: str = Field(default="cam0", min_length=1, max_length=64)
    detected_objects: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    jam_suspected: bool = False


class IntentWire(_WireModel):
    timestamp: float | None = Field(default=None, gt=0.0, allow_inf_nan=False)
    transcript: str = Field(min_length=1, max_length=4096)
    intent: str = Field(min_length=1, max_length=128)
    confidence: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)


class SensorSnapshotWire(_WireModel):
    timestamp_ms: int | None = Field(default=None, ge=0)
    motor_current_amp: float = Field(ge=0.0, allow_inf_nan=False)
    infrared_temp_celsius: float = Field(allow_inf_nan=False)
    conveyor_speed_mms: float = Field(allow_inf_nan=False)
    optical_curtain_clear: bool
    estop_circuit_closed: bool


class UICommandWire(_WireModel):
    action: str = Field(min_length=1, max_length=128)
    parameters: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "SCHEMA_VERSION",
    "CVFramePayload",
    "CVFrameWire",
    "EventEnvelope",
    "EventPayload",
    "EventSource",
    "EventType",
    "IntentPayload",
    "IntentWire",
    "JsonScalar",
    "SensorSnapshotPayload",
    "SensorSnapshotWire",
    "UICommandPayload",
    "UICommandWire",
]
