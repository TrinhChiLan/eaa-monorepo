from __future__ import annotations

import asyncio
import json
import time

import pytest
import zmq
from eaa_orchestrator.router.event_router import EventRouter

from shared.constants.zmq_topics import RouterEndpoints
from shared.schemas.events import EventSource, EventType
from tests.support import FakeSink, wait_until

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
UI_WIRE = {"action": "pause", "parameters": {"source": "test-ui"}}


async def test_direct_normalization_and_sequences() -> None:
    sink = FakeSink()
    router = EventRouter(sink, clock=lambda: 1000.0)

    cv = await router.handle_event("cv.frame", CV_WIRE)
    assert cv is not None
    assert cv.event_type is EventType.CV_FRAME
    assert cv.source is EventSource.CV
    assert cv.topic == "cv.frame"
    assert cv.sequence == 0
    assert cv.event_time == 900.0
    assert cv.ingested_at == 1000.0
    assert cv.payload.kind == "cv.frame"

    cv_again = await router.handle_event("cv.frame", CV_WIRE)
    assert cv_again is not None and cv_again.sequence == 1

    sensor = await router.handle_event("sensor.snapshot", SENSOR_WIRE)
    assert sensor is not None
    assert sensor.source is EventSource.SENSOR
    assert sensor.sequence == 0
    assert sensor.event_time == 900.5
    assert sensor.payload.motor_current_amp == 1.84  # type: ignore[union-attr]

    intent = await router.handle_event("stt.intent", json.dumps(INTENT_WIRE).encode())
    assert intent is not None
    assert intent.event_type is EventType.STT_INTENT
    assert intent.payload.intent == "clear_jam"  # type: ignore[union-attr]

    ui = await router.handle_event("ui.command", UI_WIRE)
    assert ui is not None
    assert ui.event_type is EventType.UI_COMMAND
    assert ui.payload.action == "pause"  # type: ignore[union-attr]

    assert len(sink.events) == 5
    stats = router.stats()
    assert stats.received == 5
    assert stats.emitted == 5
    assert stats.invalid == 0


async def test_timestamp_fallback_uses_clock() -> None:
    sink = FakeSink()
    router = EventRouter(sink, clock=lambda: 1234.5)
    event = await router.handle_event(
        "cv.frame",
        {"detected_objects": ["carton"], "confidence": 0.7},
    )
    assert event is not None
    assert event.event_time == 1234.5
    assert event.ingested_at == 1234.5


async def test_invalid_inputs_are_counted_not_raised() -> None:
    sink = FakeSink()
    router = EventRouter(sink, max_message_bytes=64)

    assert await router.handle_event("cv.frame", b"{not json") is None
    assert await router.handle_event("unknown.topic", CV_WIRE) is None
    assert (
        await router.handle_event("cv.frame", {"detected_objects": [], "confidence": 2.0}) is None
    )
    assert await router.handle_event("cv.frame", b"x" * 100) is None
    assert await router.handle_event("cv.frame", ["not", "an", "object"]) is None
    assert sink.events == []

    stats = router.stats()
    assert stats.invalid >= 3
    assert stats.unknown_topic == 1
    assert stats.oversized == 1
    assert stats.emitted == 0


async def test_handlers_run_and_failures_are_isolated() -> None:
    sink = FakeSink()
    router = EventRouter(sink)
    called: list[str] = []

    async def good_handler(event) -> None:
        called.append(event.event_id)

    async def bad_handler(event) -> None:
        raise RuntimeError("handler boom")

    router.register_handler("sensor.", good_handler)
    router.register_handler("sensor.", bad_handler)
    router.register_handler("cv.", good_handler)

    event = await router.handle_event("sensor.snapshot", SENSOR_WIRE)
    assert event is not None
    assert called == [event.event_id]
    assert len(sink.events) == 1
    assert router.stats().handler_errors == 1


async def test_sink_failure_is_isolated_and_prevents_emit() -> None:
    sink = FakeSink(fail=True)
    router = EventRouter(sink)
    called: list[str] = []

    async def handler(event) -> None:
        called.append(event.event_id)

    router.register_handler("", handler)
    assert await router.handle_event("sensor.snapshot", SENSOR_WIRE) is None
    stats = router.stats()
    assert stats.sink_errors == 1
    assert stats.emitted == 0
    assert called == []


async def test_start_and_stop_are_idempotent(tmp_path) -> None:
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    router = EventRouter(FakeSink(), endpoints=endpoints)
    await router.start()
    await router.start()
    await router.stop()
    await router.stop()
    assert router.stats().emitted == 0


async def test_router_can_restart_after_stop(tmp_path) -> None:
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    router = EventRouter(FakeSink(), endpoints=endpoints)
    await router.start()
    await router.stop()
    await router.start()
    await router.stop()


async def test_zmq_ingress_for_all_protocols(tmp_path) -> None:
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    sink = FakeSink()
    router = EventRouter(sink, endpoints=endpoints)
    await router.start()

    context = zmq.Context()
    cv_pub = context.socket(zmq.PUB)
    sensor_pub = context.socket(zmq.PUB)
    stt_push = context.socket(zmq.PUSH)
    ui_push = context.socket(zmq.PUSH)
    for socket in (cv_pub, sensor_pub, stt_push, ui_push):
        socket.setsockopt(zmq.LINGER, 0)

    try:
        cv_pub.bind(endpoints.cv_sub)
        sensor_pub.bind(endpoints.sensor_sub)
        stt_push.connect(endpoints.stt_pull)
        ui_push.connect(endpoints.ui_pull)

        # PUB/SUB has a slow-joiner window; retry until at least one of each
        # pub/sub topic is observed rather than assuming the first send lands.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if not any(event.topic == "cv.frame" for event in sink.events):
                cv_pub.send_multipart([b"cv.frame", json.dumps(CV_WIRE).encode("utf-8")])
            if not any(event.topic == "sensor.snapshot" for event in sink.events):
                sensor_pub.send_multipart(
                    [b"sensor.snapshot", json.dumps(SENSOR_WIRE).encode("utf-8")]
                )
            if any(event.topic == "cv.frame" for event in sink.events) and any(
                event.topic == "sensor.snapshot" for event in sink.events
            ):
                break
            await asyncio.sleep(0.05)

        stt_push.send_multipart([b"stt.intent", json.dumps(INTENT_WIRE).encode("utf-8")])
        ui_push.send_multipart([b"ui.command", json.dumps(UI_WIRE).encode("utf-8")])

        await wait_until(
            lambda: (
                {"cv.frame", "sensor.snapshot", "stt.intent", "ui.command"}
                <= {event.topic for event in sink.events}
            ),
            timeout=3.0,
            message="not all protocol messages reached the router",
        )
    finally:
        for socket in (cv_pub, sensor_pub, stt_push, ui_push):
            socket.close(linger=0)
        context.term()
        await router.stop()

    assert router.stats().emitted >= 4
    assert router.stats().invalid == 0


async def test_listener_survives_invalid_message(tmp_path) -> None:
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    sink = FakeSink()
    router = EventRouter(sink, endpoints=endpoints)
    await router.start()

    context = zmq.Context()
    stt_push = context.socket(zmq.PUSH)
    stt_push.setsockopt(zmq.LINGER, 0)
    try:
        stt_push.connect(endpoints.stt_pull)
        stt_push.send_multipart([b"stt.intent", b"{bad json"])
        await wait_until(
            lambda: router.stats().invalid == 1,
            timeout=2.0,
            message="invalid message was not counted",
        )

        stt_push.send_multipart([b"stt.intent", json.dumps(INTENT_WIRE).encode("utf-8")])
        await wait_until(
            lambda: len(sink.events) == 1,
            timeout=2.0,
            message="listener did not recover after an invalid message",
        )
    finally:
        stt_push.close(linger=0)
        context.term()
        await router.stop()


async def test_second_router_requires_exclusive_lock(tmp_path) -> None:
    endpoints = RouterEndpoints.default(tmp_path / "ipc")
    first = EventRouter(FakeSink(), endpoints=endpoints)
    second = EventRouter(FakeSink(), endpoints=endpoints)
    await first.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            await second.start()
    finally:
        await second.stop()
        await first.stop()
