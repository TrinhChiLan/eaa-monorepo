"""Shared test helpers: event factories and fakes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from shared.constants.zmq_topics import (
    TOPIC_CV_FRAME,
    TOPIC_SENSOR_SNAPSHOT,
    TOPIC_STT_INTENT,
    TOPIC_UI_COMMAND,
)
from shared.contracts import SubmitOutcome
from shared.schemas.events import (
    CVFramePayload,
    EventEnvelope,
    EventSource,
    IntentPayload,
    SensorSnapshotPayload,
    UICommandPayload,
)


def make_cv_event(
    *, sequence: int = 0, confidence: float = 0.9, event_time: float | None = None
) -> EventEnvelope:
    return EventEnvelope.create(
        payload=CVFramePayload(
            camera_id="cam0",
            detected_objects=["carton"],
            confidence=confidence,
            jam_suspected=False,
        ),
        source=EventSource.CV,
        topic=TOPIC_CV_FRAME,
        sequence=sequence,
        event_time=event_time,
    )


def make_sensor_event(
    *, sequence: int = 0, event_time: float | None = None, current: float = 1.5
) -> EventEnvelope:
    return EventEnvelope.create(
        payload=SensorSnapshotPayload(
            motor_current_amp=current,
            infrared_temp_celsius=42.0,
            conveyor_speed_mms=320.0,
            optical_curtain_clear=True,
            estop_circuit_closed=True,
            sensor_timestamp_ms=1_700_000_000_000,
        ),
        source=EventSource.SENSOR,
        topic=TOPIC_SENSOR_SNAPSHOT,
        sequence=sequence,
        event_time=event_time,
    )


def make_intent_event(*, sequence: int = 0, event_time: float | None = None) -> EventEnvelope:
    return EventEnvelope.create(
        payload=IntentPayload(
            transcript="resume the line",
            intent="resume",
            confidence=0.91,
        ),
        source=EventSource.STT,
        topic=TOPIC_STT_INTENT,
        sequence=sequence,
        event_time=event_time,
    )


def make_ui_event(*, sequence: int = 0, event_time: float | None = None) -> EventEnvelope:
    return EventEnvelope.create(
        payload=UICommandPayload(action="pause", parameters={"source": "test"}),
        source=EventSource.UI,
        topic=TOPIC_UI_COMMAND,
        sequence=sequence,
        event_time=event_time,
    )


class FakeSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[EventEnvelope] = []
        self.fail = fail

    async def push(self, event: EventEnvelope) -> None:
        if self.fail:
            raise RuntimeError("simulated sink failure")
        self.events.append(event)


class FakePersistence:
    def __init__(
        self,
        *,
        outcomes: list[SubmitOutcome] | None = None,
        default: SubmitOutcome = SubmitOutcome.QUEUED,
        error: Exception | None = None,
    ) -> None:
        self.events: list[EventEnvelope] = []
        self.outcomes = list(outcomes or [])
        self.default = default
        self.error = error

    async def submit(self, event: EventEnvelope) -> SubmitOutcome:
        if self.error is not None:
            raise self.error
        self.events.append(event)
        if self.outcomes:
            return self.outcomes.pop(0)
        return self.default


class FakeRepository:
    """Duck-typed repository for worker tests (no SQLite)."""

    def __init__(self, *, fail_times: int = 0, prune_result: int = 0) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.events: list[EventEnvelope] = []
        self.prune_calls: list[dict[str, Any]] = []
        self.prune_result = prune_result

    async def bulk_insert(self, events: list[EventEnvelope]) -> int:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("simulated SQLite failure")
        self.events.extend(events)
        return len(events)

    async def prune(
        self,
        *,
        max_rows: int | None = None,
        max_age_seconds: float | None = None,
        now: float | None = None,
    ) -> int:
        self.prune_calls.append(
            {
                "max_rows": max_rows,
                "max_age_seconds": max_age_seconds,
                "now": now,
            }
        )
        return self.prune_result


async def wait_until(
    predicate: Callable[[], bool | Any],
    *,
    timeout: float = 3.0,  # noqa: ASYNC109 - test helper, not an async API
    interval: float = 0.01,
    message: str = "condition not met before timeout",
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)
    raise AssertionError(message)


def epoch(offset: float = 0.0) -> float:
    """Stable, positive event timestamp for ordering tests."""
    return 1_700_000_000.0 + offset
