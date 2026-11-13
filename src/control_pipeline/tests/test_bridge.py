"""Unit tests for McuBridgeWorker and MockMcu."""

from __future__ import annotations

import asyncio
import pytest

from control_pipeline.bridge.mock_mcu import MockMcu
from control_pipeline.bridge.transport import MockMcuTransport
from control_pipeline.bridge.worker import McuBridgeWorker
from control_pipeline.schemas.bridge import LinkState, McuEventFrame, McuEventKind, TelemetryFrame
from control_pipeline.schemas.fsm import Action


@pytest.mark.asyncio
async def test_hello_handshake():
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    worker = McuBridgeWorker(mpu_tr, hello_retry_interval_s=0.1)

    await mock.start()
    await worker.start()

    try:
        # Give enough time for handshake
        await asyncio.sleep(0.3)
        assert worker.link_state == LinkState.UP
        assert worker.health.mcu_boot_id == 0x1234
    finally:
        await worker.stop()
        await mock.stop()


@pytest.mark.asyncio
async def test_telemetry_streaming():
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    received_telemetry: list[TelemetryFrame] = []

    async def on_telem(tf: TelemetryFrame) -> None:
        received_telemetry.append(tf)

    worker = McuBridgeWorker(mpu_tr, telemetry_listener=on_telem, hello_retry_interval_s=0.1)

    await mock.start()
    await worker.start()

    try:
        await asyncio.sleep(0.4)
        assert len(received_telemetry) >= 2
        assert received_telemetry[-1].current_ma == 850
        assert worker.health.telemetry_frames_rx >= 2
    finally:
        await worker.stop()
        await mock.stop()


@pytest.mark.asyncio
async def test_command_execution():
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    worker = McuBridgeWorker(mpu_tr)

    await mock.start()
    await worker.start()

    try:
        await asyncio.sleep(0.2)
        # Send START action
        ack = await worker.send_action(Action.START, setpoint_mms=150)
        assert ack is not None
        assert ack.result == 0  # ACCEPTED
        assert mock.mcu_state.name == "RUNNING"
        assert mock.setpoint_mms == 150

        # Send SET_SPEED action
        ack_speed = await worker.send_action(Action.SET_SPEED, setpoint_mms=300)
        assert ack_speed is not None
        assert ack_speed.result == 0
        assert mock.setpoint_mms == 300

        # Send STOP action
        ack_stop = await worker.send_action(Action.STOP)
        assert ack_stop is not None
        assert ack_stop.result == 0
        assert mock.mcu_state.name == "STOPPED"
    finally:
        await worker.stop()
        await mock.stop()


@pytest.mark.asyncio
async def test_mcu_event_and_event_ack():
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    events_received: list[McuEventFrame] = []

    async def on_event(ev: McuEventFrame) -> None:
        events_received.append(ev)

    worker = McuBridgeWorker(mpu_tr, mcu_event_listener=on_event)

    await mock.start()
    await worker.start()

    try:
        await asyncio.sleep(0.2)
        # Inject Jam
        mock.inject_jam()
        await asyncio.sleep(0.2)

        assert len(events_received) >= 1
        assert events_received[0].event_kind == McuEventKind.JAM_DETECTED
        assert worker.health.event_acks_sent >= 1
    finally:
        await worker.stop()
        await mock.stop()


@pytest.mark.asyncio
async def test_link_loss_detection():
    mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
    mock = MockMcu(mcu_tr)
    link_lost_called = False

    async def on_link_lost() -> None:
        nonlocal link_lost_called
        link_lost_called = True

    worker = McuBridgeWorker(
        mpu_tr,
        stale_threshold_ms=100.0,
        lost_threshold_ms=250.0,
        link_lost_callback=on_link_lost,
    )

    await mock.start()
    await worker.start()

    try:
        await asyncio.sleep(0.2)
        assert worker.link_state == LinkState.UP

        # Stop MCU to simulate communication cut
        await mock.stop()

        # Wait past lost threshold
        await asyncio.sleep(0.35)
        assert worker.link_state == LinkState.LOST
        assert link_lost_called is True
    finally:
        await worker.stop()
