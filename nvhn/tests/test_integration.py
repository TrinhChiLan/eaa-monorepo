from __future__ import annotations

import json

import zmq
from eaa_orchestrator.persistence.database import Database
from eaa_orchestrator.persistence.repository import (
    HalfSyncHalfAsyncPersistenceWorker,
    TelemetryRepository,
)
from eaa_orchestrator.router.event_router import EventRouter
from eaa_orchestrator.telemetry.ring_buffer import TelemetryRingBuffer

from shared.config import DEFAULT_PERSIST_EVENT_TYPES
from shared.constants.zmq_topics import RouterEndpoints
from shared.schemas.events import EventType
from tests.support import make_sensor_event, wait_until

CV_WIRE = {
    "timestamp": 900.0,
    "camera_id": "cam0",
    "detected_objects": ["carton", "jam"],
    "confidence": 0.93,
    "jam_suspected": True,
}
SENSOR_WIRE = {
    "timestamp_ms": 900_500,
    "motor_current_amp": 1.84,
    "infrared_temp_celsius": 41.2,
    "conveyor_speed_mms": 320.0,
    "optical_curtain_clear": True,
    "estop_circuit_closed": True,
}
INTENT_WIRE = {
    "timestamp": 901.0,
    "transcript": "clear jam and resume",
    "intent": "clear_jam",
    "confidence": 0.88,
}
UI_WIRE = {"action": "pause", "parameters": {"source": "integration"}}


async def _count_is(repository: TelemetryRepository, expected: int) -> bool:
    return await repository.count() == expected


async def test_router_to_sqlite_with_default_allowlist(db_path, tmp_path) -> None:
    database = Database(db_path)
    repository = TelemetryRepository(database)
    await repository.initialize()

    worker = HalfSyncHalfAsyncPersistenceWorker(
        repository,
        queue_capacity=100,
        batch_size=2,
        flush_interval=0.05,
        persist_event_types=DEFAULT_PERSIST_EVENT_TYPES,
        retry_backoff=0.01,
    )
    await worker.start()
    buffer = TelemetryRingBuffer(capacity=100, persistence=worker)
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    router = EventRouter(buffer, endpoints=endpoints)
    await router.start()

    context = zmq.Context()
    stt_push = context.socket(zmq.PUSH)
    ui_push = context.socket(zmq.PUSH)
    for socket in (stt_push, ui_push):
        socket.setsockopt(zmq.LINGER, 0)

    try:
        stt_push.connect(endpoints.stt_pull)
        ui_push.connect(endpoints.ui_pull)

        await router.handle_event("cv.frame", CV_WIRE)
        await router.handle_event("sensor.snapshot", SENSOR_WIRE)
        await router.handle_event("sensor.snapshot", {**SENSOR_WIRE, "motor_current_amp": 2.1})
        stt_push.send_multipart([b"stt.intent", json.dumps(INTENT_WIRE).encode("utf-8")])
        ui_push.send_multipart([b"ui.command", json.dumps(UI_WIRE).encode("utf-8")])

        await wait_until(
            lambda: router.stats().emitted >= 5,
            timeout=3.0,
            message="router did not emit all five events",
        )
        await wait_until(
            lambda: _count_is(repository, 4),
            timeout=3.0,
            message="default allowlist did not persist four non-CV events",
        )
    finally:
        stt_push.close(linger=0)
        ui_push.close(linger=0)
        context.term()
        await router.stop()
        await worker.stop(drain_timeout=2.0)

    try:
        assert await repository.count() == 4
        assert await repository.count(event_type=EventType.CV_FRAME) == 0
        assert await repository.count(event_type=EventType.SENSOR_SNAPSHOT) == 2
        assert await repository.count(event_type=EventType.STT_INTENT) == 1
        assert await repository.count(event_type=EventType.UI_COMMAND) == 1

        ring = await buffer.stats()
        assert ring.size == 5
        assert ring.persistence_filtered == 1
        assert ring.persistence_queued == 4
        assert worker.stats().failed_batches == 0

        snapshot = await buffer.snapshot()
        assert {event.topic for event in snapshot} == {
            "cv.frame",
            "sensor.snapshot",
            "stt.intent",
            "ui.command",
        }

        fetched = await repository.fetch_recent(limit=10)
        assert len(fetched) == 4
        assert all(event.event_type is not EventType.CV_FRAME for event in fetched)
    finally:
        await repository.close()


async def test_cv_frames_persist_when_explicitly_enabled(db_path, tmp_path) -> None:
    database = Database(db_path)
    repository = TelemetryRepository(database)
    await repository.initialize()

    all_types = frozenset(
        {
            EventType.CV_FRAME,
            EventType.SENSOR_SNAPSHOT,
            EventType.STT_INTENT,
            EventType.UI_COMMAND,
        }
    )
    worker = HalfSyncHalfAsyncPersistenceWorker(
        repository,
        batch_size=1,
        flush_interval=0.05,
        persist_event_types=all_types,
        retry_backoff=0.01,
    )
    await worker.start()
    buffer = TelemetryRingBuffer(capacity=10, persistence=worker)
    router = EventRouter(buffer, endpoints=RouterEndpoints.default(tmp_path / "ipc"))

    try:
        await router.handle_event("cv.frame", CV_WIRE)
        await wait_until(
            lambda: _count_is(repository, 1),
            timeout=3.0,
            message="CV frame was not persisted when enabled",
        )
    finally:
        await router.stop()
        await worker.stop(drain_timeout=2.0)

    try:
        assert await repository.count(event_type=EventType.CV_FRAME) == 1
        fetched = await repository.fetch_recent(limit=1)
        assert fetched[0].event_type is EventType.CV_FRAME
    finally:
        await repository.close()


async def test_graceful_shutdown_flushes_queued_events(db_path) -> None:
    database = Database(db_path)
    repository = TelemetryRepository(database)
    await repository.initialize()
    worker = HalfSyncHalfAsyncPersistenceWorker(
        repository,
        queue_capacity=100,
        batch_size=50,
        flush_interval=10.0,
        retry_backoff=0.01,
    )
    await worker.start()
    buffer = TelemetryRingBuffer(capacity=100, persistence=worker)

    try:
        for index in range(20):
            await buffer.push(make_sensor_event(sequence=index))
        await worker.stop(drain_timeout=2.0)
        assert await repository.count() == 20
        stats = worker.stats()
        assert stats.unflushed_on_shutdown == 0
    finally:
        await repository.close()
