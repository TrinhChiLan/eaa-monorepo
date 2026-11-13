"""Unit tests for SafetyInvariantArbiter."""

from __future__ import annotations

import pytest

from control_pipeline.safety.arbiter import SafetyInvariantArbiter
from control_pipeline.schemas.arbiter import ReasonCode, SensorReading
from control_pipeline.schemas.fsm import Action
from control_pipeline.tests.conftest import FakeSensorSource


def _make_source(override: dict | None = None) -> tuple[FakeSensorSource, SensorReading]:
    base = SensorReading(
        estop_closed=True,
        pe_clear=True,
        hb_valid=True,
        fault_latched=False,
        motor_enabled=False,
        sensors_ok=True,
        motor_current_ma=850.0,
        conveyor_speed_mms=0.0,
        infrared_temp_celsius=34.2,
        sensor_timestamp_ms=1000,
        link_state="UP",
        last_frame_age_ms=50.0,
    )
    if override:
        base = base.model_copy(update=override)
    return FakeSensorSource(base), base


@pytest.mark.asyncio
async def test_start_permitted_when_healthy():
    source, _ = _make_source()
    arbiter = SafetyInvariantArbiter(source)
    decision = await arbiter.evaluate(Action.START, setpoint_mms=150, origin="ui", payload={})
    assert decision.permitted is True
    assert decision.reason_code == ReasonCode.OK_NOMINAL


@pytest.mark.asyncio
async def test_estop_open_denies_start():
    source, _ = _make_source({"estop_closed": False})
    arbiter = SafetyInvariantArbiter(source)
    decision = await arbiter.evaluate(Action.START, setpoint_mms=150, origin="ui", payload={})
    assert decision.permitted is False
    assert decision.reason_code == ReasonCode.ERR_ESTOP_ACTIVE


@pytest.mark.asyncio
async def test_halt_always_permitted_even_if_estop_open():
    source, _ = _make_source({"estop_closed": False})
    arbiter = SafetyInvariantArbiter(source)
    decision = await arbiter.evaluate(Action.HALT, setpoint_mms=None, origin="ui", payload={})
    assert decision.permitted is True


@pytest.mark.asyncio
async def test_pe_blocked_permits_start_but_blocks_recover():
    source, _ = _make_source({"pe_clear": False})
    arbiter = SafetyInvariantArbiter(source)

    # Invariant I-2: Normal START is PERMITTED (workpiece on stopped belt is valid)
    start_dec = await arbiter.evaluate(Action.START, setpoint_mms=150, origin="ui", payload={})
    assert start_dec.permitted is True

    # But RECOVER requires photoelectric beam to be CLEAR
    recover_dec = await arbiter.evaluate(Action.RECOVER, setpoint_mms=None, origin="ui", payload={})
    assert recover_dec.permitted is False
    assert recover_dec.reason_code == ReasonCode.ERR_PHOTOELECTRIC_BLOCKED


@pytest.mark.asyncio
async def test_overcurrent_denies_energizing():
    source, _ = _make_source({"motor_current_ma": 3200.0})
    arbiter = SafetyInvariantArbiter(source)
    decision = await arbiter.evaluate(Action.START, setpoint_mms=150, origin="ui", payload={})
    assert decision.permitted is False
    assert decision.reason_code == ReasonCode.CURRENT_NOT_NOMINAL


@pytest.mark.asyncio
async def test_link_stale_permits_stop_but_denies_start():
    source, _ = _make_source({"link_state": "STALE"})
    arbiter = SafetyInvariantArbiter(source)

    start_dec = await arbiter.evaluate(Action.START, setpoint_mms=150, origin="ui", payload={})
    assert start_dec.permitted is False
    assert start_dec.reason_code == ReasonCode.LINK_STALE

    stop_dec = await arbiter.evaluate(Action.STOP, setpoint_mms=None, origin="ui", payload={})
    assert stop_dec.permitted is True


@pytest.mark.asyncio
async def test_speed_limit_validation():
    source, _ = _make_source()
    arbiter = SafetyInvariantArbiter(source, min_speed_mms=50, max_speed_mms=500)

    # Valid setpoint
    dec = await arbiter.evaluate(Action.SET_SPEED, setpoint_mms=250, origin="ui", payload={})
    assert dec.permitted is True

    # Below min
    dec_low = await arbiter.evaluate(Action.SET_SPEED, setpoint_mms=30, origin="ui", payload={})
    assert dec_low.permitted is False
    assert dec_low.reason_code == ReasonCode.ERR_BAD_PARAM

    # Above max
    dec_high = await arbiter.evaluate(Action.SET_SPEED, setpoint_mms=600, origin="ui", payload={})
    assert dec_high.permitted is False
    assert dec_high.reason_code == ReasonCode.ERR_BAD_PARAM


@pytest.mark.asyncio
async def test_stt_confidence_filtering():
    source, _ = _make_source()
    arbiter = SafetyInvariantArbiter(source, min_stt_confidence=0.80)

    # Low confidence voice intent is rejected
    dec_low = await arbiter.evaluate(
        Action.START, setpoint_mms=150, origin="stt", payload={"confidence": 0.65}
    )
    assert dec_low.permitted is False
    assert dec_low.reason_code == ReasonCode.LOW_CONFIDENCE

    # High confidence voice intent is accepted
    dec_high = await arbiter.evaluate(
        Action.START, setpoint_mms=150, origin="stt", payload={"confidence": 0.95}
    )
    assert dec_high.permitted is True
