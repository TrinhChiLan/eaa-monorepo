"""Integration tests for ContinuousPipeline linking event_pipeline and control_pipeline."""

from __future__ import annotations

import asyncio
import time
import uuid
import pytest

from continuous_pipeline import ContinuousPipeline
from control_pipeline.schemas.fsm import SystemState
from mock_data.constants.zmq_topics import TOPIC_CV_FRAME, TOPIC_STT_INTENT, TOPIC_UI_COMMAND
from mock_data.schemas.events import (
    CVFramePayload,
    EventEnvelope,
    EventSource,
    EventType,
    IntentPayload,
    UICommandPayload,
)


@pytest.mark.asyncio
async def test_continuous_pipeline_lifecycle():
    pipeline = ContinuousPipeline(db_path=":memory:")
    await pipeline.start()

    try:
        assert pipeline.control.fsm_state == SystemState.IDLE
        assert pipeline.ring_buffer is not None
        assert pipeline.persistence_worker is not None
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_ui_command_drives_fsm_and_streams_telemetry():
    pipeline = ContinuousPipeline(db_path=":memory:")
    await pipeline.start()

    try:
        # Wait for handshake
        await asyncio.sleep(0.3)

        # 1. Push UI START command through the EventRouter sink interface
        now = time.time()
        start_envelope = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=1,
            event_time=now,
            ingested_at=now,
            payload=UICommandPayload(action="start", parameters={"setpoint_mms": 150}),
        )

        await pipeline.push(start_envelope)

        # Assert FSM transitioned from IDLE to RUNNING
        assert pipeline.control.fsm_state == SystemState.RUNNING

        # 2. Wait for 20Hz telemetry to stream from MockMcu into TelemetryRingBuffer
        await asyncio.sleep(0.3)

        snapshots = await pipeline.ring_buffer.get_latest(count=5)
        assert len(snapshots) > 0
        # Check that telemetry snapshots reached the ring buffer
        telemetry_snapshots = [s for s in snapshots if s.event_type == EventType.SENSOR_SNAPSHOT]
        assert len(telemetry_snapshots) > 0
        latest_sensor = telemetry_snapshots[-1].payload
        assert latest_sensor.motor_current_amp > 0

        # 3. Stop belt
        stop_envelope = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=2,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=UICommandPayload(action="stop"),
        )
        await pipeline.push(stop_envelope)
        assert pipeline.control.fsm_state == SystemState.IDLE

    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_stt_voice_intent_and_cv_jam():
    pipeline = ContinuousPipeline(db_path=":memory:")
    await pipeline.start()

    try:
        await asyncio.sleep(0.3)

        # Start belt first
        start_envelope = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.UI_COMMAND,
            source=EventSource.UI,
            topic=TOPIC_UI_COMMAND,
            sequence=1,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=UICommandPayload(action="start"),
        )
        await pipeline.push(start_envelope)
        assert pipeline.control.fsm_state == SystemState.RUNNING

        # Voice PAUSE with high confidence (0.95)
        pause_voice = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.STT_INTENT,
            source=EventSource.STT,
            topic=TOPIC_STT_INTENT,
            sequence=2,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=IntentPayload(intent="pause", transcript="pause conveyor", confidence=0.95),
        )
        await pipeline.push(pause_voice)
        assert pipeline.control.fsm_state == SystemState.PAUSED

        # Voice RESUME with high confidence (0.92)
        resume_voice = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.STT_INTENT,
            source=EventSource.STT,
            topic=TOPIC_STT_INTENT,
            sequence=3,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=IntentPayload(intent="resume", transcript="resume line", confidence=0.92),
        )
        await pipeline.push(resume_voice)
        assert pipeline.control.fsm_state == SystemState.RUNNING

        # CV Jam detection
        cv_jam = EventEnvelope(
            schema_version=1,
            event_id=uuid.uuid4().hex,
            event_type=EventType.CV_FRAME,
            source=EventSource.CV,
            topic=TOPIC_CV_FRAME,
            sequence=4,
            event_time=time.time(),
            ingested_at=time.time(),
            payload=CVFramePayload(camera_id="cam_main", jam_suspected=True, confidence=0.98),
        )
        await pipeline.push(cv_jam)
        assert pipeline.control.fsm_state == SystemState.JAMMED

    finally:
        await pipeline.stop()
