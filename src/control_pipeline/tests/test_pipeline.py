"""Integration tests for the OrchestratorPipeline."""

from __future__ import annotations

import asyncio
import pytest

from control_pipeline.config import OrchestratorSettings
from control_pipeline.pipeline import OrchestratorPipeline
from control_pipeline.schemas.arbiter import SensorReading
from control_pipeline.schemas.fsm import FsmTrigger, SystemState


@pytest.mark.asyncio
async def test_full_start_and_stop_lifecycle():
    settings = OrchestratorSettings(anti_flap_lockout_s=0.1)
    pipeline = OrchestratorPipeline(settings)
    await pipeline.start()

    try:
        await asyncio.sleep(0.3)  # wait for handshake
        assert pipeline.fsm_state == SystemState.IDLE

        # 1. START
        res = await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
        assert res["accepted"] is True
        assert pipeline.fsm_state == SystemState.RUNNING

        await asyncio.sleep(0.2)
        assert pipeline.mock_mcu.mcu_state.name == "RUNNING"

        # 2. SET_SPEED
        res_speed = await pipeline.submit_event(
            FsmTrigger.SET_SPEED_CMD, origin="ui", payload={"setpoint_mms": 220}
        )
        assert res_speed["accepted"] is True
        assert pipeline.fsm_state == SystemState.RUNNING

        await asyncio.sleep(0.2)
        assert pipeline.mock_mcu.setpoint_mms == 220

        # 3. STOP
        res_stop = await pipeline.submit_event(FsmTrigger.STOP_CMD, origin="ui")
        assert res_stop["accepted"] is True
        assert pipeline.fsm_state == SystemState.IDLE

        await asyncio.sleep(0.2)
        assert pipeline.mock_mcu.mcu_state.name == "STOPPED"
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_pause_and_resume_lifecycle():
    pipeline = OrchestratorPipeline()
    await pipeline.start()

    try:
        await asyncio.sleep(0.3)
        await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
        assert pipeline.fsm_state == SystemState.RUNNING

        # PAUSE
        res_pause = await pipeline.submit_event(FsmTrigger.PAUSE_CMD, origin="ui")
        assert res_pause["accepted"] is True
        assert pipeline.fsm_state == SystemState.PAUSED

        # RESUME
        res_resume = await pipeline.submit_event(FsmTrigger.RESUME_CMD, origin="ui")
        assert res_resume["accepted"] is True
        assert pipeline.fsm_state == SystemState.RUNNING
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_estop_fault_and_recovery():
    pipeline = OrchestratorPipeline()
    await pipeline.start()

    try:
        await asyncio.sleep(0.3)
        await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
        assert pipeline.fsm_state == SystemState.RUNNING

        # Inject E-Stop
        pipeline.mock_mcu.inject_estop()
        await pipeline.submit_event(FsmTrigger.SAFETY_EVENT, origin="hardware")
        assert pipeline.fsm_state == SystemState.SAFETY_FAULT

        # Attempt to restart while E-stop active -> should fail
        res = await pipeline.submit_event(FsmTrigger.START_CMD, origin="ui")
        assert res["accepted"] is False

        # Restore E-stop and clear fault
        pipeline.mock_mcu.restore()
        res_clear = await pipeline.submit_event(FsmTrigger.FAULT_CLEARED, origin="mcu")
        assert res_clear["accepted"] is True
        assert pipeline.fsm_state == SystemState.IDLE
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_stt_confidence_gate():
    pipeline = OrchestratorPipeline(OrchestratorSettings(min_stt_confidence=0.80))
    await pipeline.start()

    try:
        await asyncio.sleep(0.3)

        # Low confidence voice start -> blocked by arbiter
        res_low = await pipeline.submit_event(
            FsmTrigger.START_CMD, origin="stt", payload={"confidence": 0.50}
        )
        assert res_low["accepted"] is True  # FSM accepted transition structurally
        assert res_low["arbiter_decision"]["permitted"] is False  # Arbiter inhibited MCU command
        # MCU was NOT started
        assert pipeline.mock_mcu.mcu_state.name == "STOPPED"
    finally:
        await pipeline.stop()
