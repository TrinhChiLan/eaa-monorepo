from __future__ import annotations

import asyncio

import pytest
from eaa_orchestrator.telemetry.ring_buffer import TelemetryRingBuffer

from shared.contracts import SubmitOutcome
from tests.support import (
    FakePersistence,
    epoch,
    make_cv_event,
    make_sensor_event,
)


async def test_capacity_eviction_and_stats() -> None:
    buffer = TelemetryRingBuffer(capacity=3)
    for index in range(5):
        await buffer.push(make_sensor_event(sequence=index, event_time=epoch(index)))

    stats = await buffer.stats()
    assert stats.size == 3
    assert stats.total_pushed == 5
    assert stats.evicted == 2
    assert stats.oldest_event_time == epoch(2)
    assert stats.newest_event_time == epoch(4)

    snapshot = await buffer.snapshot()
    assert [event.sequence for event in snapshot] == [2, 3, 4]


async def test_get_latest_and_snapshot_ordering() -> None:
    buffer = TelemetryRingBuffer(capacity=10)
    for index in range(5):
        await buffer.push(make_sensor_event(sequence=index, event_time=epoch(index)))

    latest = await buffer.get_latest(2)
    assert [event.sequence for event in latest] == [3, 4]

    limited = await buffer.snapshot(3)
    assert [event.sequence for event in limited] == [2, 3, 4]

    assert await buffer.get_latest(0) == []
    assert await buffer.get_latest(-5) == []
    assert await buffer.snapshot(0) == []


async def test_concurrent_pushes_do_not_lose_accepted_events() -> None:
    buffer = TelemetryRingBuffer(capacity=1000)

    async def push(index: int) -> None:
        await buffer.push(make_cv_event(sequence=index, event_time=epoch(index)))

    await asyncio.gather(*(push(index) for index in range(200)))
    stats = await buffer.stats()
    assert stats.size == 200
    assert stats.total_pushed == 200
    assert stats.evicted == 0


async def test_persistence_outcomes_are_counted() -> None:
    persistence = FakePersistence(
        outcomes=[
            SubmitOutcome.QUEUED,
            SubmitOutcome.FILTERED,
            SubmitOutcome.DROPPED,
            SubmitOutcome.STOPPED,
        ]
    )
    buffer = TelemetryRingBuffer(capacity=10, persistence=persistence)
    for index in range(4):
        await buffer.push(make_sensor_event(sequence=index))

    stats = await buffer.stats()
    assert stats.persistence_queued == 1
    assert stats.persistence_filtered == 1
    assert stats.persistence_dropped == 2
    assert len(persistence.events) == 4


async def test_persistence_exception_does_not_drop_event() -> None:
    persistence = FakePersistence(error=RuntimeError("db unavailable"))
    buffer = TelemetryRingBuffer(capacity=10, persistence=persistence)
    await buffer.push(make_sensor_event())

    stats = await buffer.stats()
    assert stats.size == 1
    assert stats.persistence_errors == 1
    assert stats.persistence_queued == 0


async def test_clear_keeps_cumulative_counters() -> None:
    buffer = TelemetryRingBuffer(capacity=2)
    await buffer.push(make_sensor_event(sequence=0))
    await buffer.push(make_sensor_event(sequence=1))
    await buffer.clear()

    stats = await buffer.stats()
    assert stats.size == 0
    assert stats.total_pushed == 2
    assert await buffer.snapshot() == []


async def test_rejects_non_envelope_and_bad_capacity() -> None:
    with pytest.raises(ValueError):
        TelemetryRingBuffer(capacity=0)

    buffer = TelemetryRingBuffer()
    with pytest.raises(TypeError):
        await buffer.push({"not": "an envelope"})  # type: ignore[arg-type]
