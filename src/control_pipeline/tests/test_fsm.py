"""Unit tests for FSMEngine."""

from __future__ import annotations

import asyncio
import time
import pytest

from control_pipeline.contracts import FsmObserver
from control_pipeline.fsm.engine import FSMEngine
from control_pipeline.schemas.fsm import (
    FsmEvent,
    FsmStateChanged,
    FsmTrigger,
    SystemState,
)


@pytest.mark.asyncio
async def test_initial_state():
    engine = FSMEngine(initial_state=SystemState.IDLE)
    assert engine.state == SystemState.IDLE


@pytest.mark.asyncio
async def test_start_and_stop():
    engine = FSMEngine(initial_state=SystemState.IDLE)

    ev_start = FsmEvent(trigger=FsmTrigger.START_CMD, origin="ui", payload={}, timestamp=time.time())
    res = await engine.process(ev_start)
    assert res is not None
    assert res.previous == SystemState.IDLE
    assert res.current == SystemState.RUNNING
    assert engine.state == SystemState.RUNNING

    ev_stop = FsmEvent(trigger=FsmTrigger.STOP_CMD, origin="ui", payload={}, timestamp=time.time())
    res = await engine.process(ev_stop)
    assert res is not None
    assert res.current == SystemState.IDLE
    assert engine.state == SystemState.IDLE


@pytest.mark.asyncio
async def test_pause_and_resume():
    engine = FSMEngine(initial_state=SystemState.RUNNING)

    ev_pause = FsmEvent(trigger=FsmTrigger.PAUSE_CMD, origin="ui", payload={}, timestamp=time.time())
    res = await engine.process(ev_pause)
    assert res is not None
    assert res.current == SystemState.PAUSED

    ev_resume = FsmEvent(trigger=FsmTrigger.RESUME_CMD, origin="ui", payload={}, timestamp=time.time())
    res = await engine.process(ev_resume)
    assert res is not None
    assert res.current == SystemState.RUNNING


@pytest.mark.asyncio
async def test_jam_and_guided_recovery():
    engine = FSMEngine(initial_state=SystemState.RUNNING)

    # 1. Jam detected
    res = await engine.process(
        FsmEvent(trigger=FsmTrigger.JAM_DETECTED, origin="sensor", payload={}, timestamp=time.time())
    )
    assert res is not None
    assert res.current == SystemState.JAMMED

    # 2. Operator clears blockage on wizard
    res = await engine.process(
        FsmEvent(trigger=FsmTrigger.VERIFY_CLEARANCE, origin="ui", payload={}, timestamp=time.time())
    )
    assert res is not None
    assert res.current == SystemState.RECOVERING

    # 3. Fault cleared restores to IDLE (NEVER directly to RUNNING)
    res = await engine.process(
        FsmEvent(trigger=FsmTrigger.FAULT_CLEARED, origin="mcu", payload={}, timestamp=time.time())
    )
    assert res is not None
    assert res.current == SystemState.IDLE


@pytest.mark.asyncio
@pytest.mark.parametrize("state", list(SystemState))
async def test_safety_event_from_any_state(state: SystemState):
    engine = FSMEngine(initial_state=state)
    res = await engine.process(
        FsmEvent(trigger=FsmTrigger.SAFETY_EVENT, origin="hardware", payload={}, timestamp=time.time())
    )
    assert res is not None
    assert res.current == SystemState.SAFETY_FAULT


@pytest.mark.asyncio
async def test_illegal_trigger_rejected():
    engine = FSMEngine(initial_state=SystemState.IDLE)
    # RESUME from IDLE is invalid
    res = await engine.process(
        FsmEvent(trigger=FsmTrigger.RESUME_CMD, origin="ui", payload={}, timestamp=time.time())
    )
    assert res is None
    assert engine.state == SystemState.IDLE


@pytest.mark.asyncio
async def test_anti_flap_lockout():
    engine = FSMEngine(initial_state=SystemState.IDLE, anti_flap_lockout_s=2.0)

    # First START succeeds
    res1 = await engine.process(
        FsmEvent(trigger=FsmTrigger.START_CMD, origin="ui", payload={}, timestamp=time.time())
    )
    assert res1 is not None

    # STOP back to IDLE
    await engine.process(
        FsmEvent(trigger=FsmTrigger.STOP_CMD, origin="ui", payload={}, timestamp=time.time())
    )

    # Rapid second START within 2.0s is locked out
    res2 = await engine.process(
        FsmEvent(trigger=FsmTrigger.START_CMD, origin="ui", payload={}, timestamp=time.time())
    )
    assert res2 is None
    assert engine.state == SystemState.IDLE


class SpyObserver(FsmObserver):
    def __init__(self):
        self.events: list[FsmStateChanged] = []

    async def on_state_changed(self, event: FsmStateChanged) -> None:
        self.events.append(event)


@pytest.mark.asyncio
async def test_observer_notified():
    spy = SpyObserver()
    engine = FSMEngine(initial_state=SystemState.IDLE, observers=[spy])

    await engine.process(
        FsmEvent(trigger=FsmTrigger.START_CMD, origin="ui", payload={}, timestamp=time.time())
    )
    assert len(spy.events) == 1
    assert spy.events[0].previous == SystemState.IDLE
    assert spy.events[0].current == SystemState.RUNNING


@pytest.mark.asyncio
async def test_mcu_voice_stop_from_running():
    """Verify MCU_VOICE_STOP directly restores RUNNING to IDLE (v1.1)."""
    engine = FSMEngine(initial_state=SystemState.RUNNING)
    ev = FsmEvent(
        trigger=FsmTrigger.MCU_VOICE_STOP,
        origin="mcu",
        payload={"kind": 1},
        timestamp=time.time(),
    )
    res = await engine.process(ev)
    assert res is not None
    assert res.previous == SystemState.RUNNING
    assert res.current == SystemState.IDLE
    assert engine.state == SystemState.IDLE


@pytest.mark.asyncio
async def test_mcu_voice_stop_from_paused():
    """Verify MCU_VOICE_STOP restores PAUSED to IDLE (v1.1)."""
    engine = FSMEngine(initial_state=SystemState.PAUSED)
    ev = FsmEvent(
        trigger=FsmTrigger.MCU_VOICE_STOP,
        origin="mcu",
        payload={"kind": 1},
        timestamp=time.time(),
    )
    res = await engine.process(ev)
    assert res is not None
    assert res.previous == SystemState.PAUSED
    assert res.current == SystemState.IDLE
    assert engine.state == SystemState.IDLE
