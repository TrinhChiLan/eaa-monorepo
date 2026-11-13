"""pipeline.py — Integration and pipeline wiring for the EAA orchestrator block.

Wires together:
1. FSMEngine (table-driven deterministic state machine)
2. SafetyInvariantArbiter (safety gate enforcing physical invariants)
3. McuBridgeWorker (B-03 v1.0 MPU-MCU protocol bridge)
4. FeedableSensorSource (in-memory sensor provider connecting telemetry to the arbiter)

Provides an easily modifiable input interface:
- `submit_event(trigger, origin, payload)`: Injects perception/HMI events.
- `feed_sensor(reading)`: Injects or overrides sensor readings.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from control_pipeline.bridge.mock_mcu import MockMcu
from control_pipeline.bridge.transport import ByteTransport, MockMcuTransport
from control_pipeline.bridge.worker import McuBridgeWorker
from control_pipeline.config import OrchestratorSettings
from control_pipeline.contracts import FsmObserver
from control_pipeline.fsm.engine import FSMEngine
from control_pipeline.safety.arbiter import SafetyInvariantArbiter
from control_pipeline.schemas.arbiter import ArbiterDecision, ReasonCode, SensorReading
from control_pipeline.schemas.bridge import BridgeHealth, TelemetryFrame
from control_pipeline.schemas.fsm import (
    Action,
    FsmEvent,
    FsmStateChanged,
    FsmTrigger,
    SystemState,
)


class FeedableSensorSource:
    """An in-memory SensorSource that can be dynamically updated by bridge telemetry or tests."""

    def __init__(self, initial_reading: SensorReading | None = None) -> None:
        self._reading: SensorReading = initial_reading or SensorReading(
            estop_closed=True,
            pe_clear=True,
            hb_valid=True,
            fault_latched=False,
            motor_enabled=False,
            sensors_ok=True,
            motor_current_ma=0.0,
            conveyor_speed_mms=0.0,
            infrared_temp_celsius=25.0,
            sensor_timestamp_ms=None,
            link_state="up",
            last_frame_age_ms=10.0,
        )
        self._lock = asyncio.Lock()

    async def latest(self) -> SensorReading:
        async with self._lock:
            return self._reading

    async def feed(self, reading: SensorReading) -> None:
        async with self._lock:
            self._reading = reading


class OrchestratorPipeline(FsmObserver):
    """Integrated pipeline managing FSM, Safety Arbiter, and Bridge Worker."""

    def __init__(
        self,
        settings: OrchestratorSettings | None = None,
        *,
        transport: ByteTransport | None = None,
        mock_mcu: MockMcu | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._settings = settings or OrchestratorSettings()
        self._logger = logger or logging.getLogger(__name__)

        # Sensor source
        self._sensor_source = FeedableSensorSource()

        # Arbiter
        self._arbiter = SafetyInvariantArbiter(
            self._sensor_source,
            max_current_ma=self._settings.max_current_ma,
            max_temp_celsius=self._settings.max_temp_celsius,
            min_speed_mms=self._settings.min_speed_mms,
            max_speed_mms=self._settings.max_speed_mms,
            min_stt_confidence=self._settings.min_stt_confidence,
            max_intent_age_ms=self._settings.max_intent_age_ms,
            logger=self._logger.getChild("arbiter"),
        )

        # FSM Engine
        init_state = SystemState(self._settings.initial_state)
        self._fsm = FSMEngine(
            initial_state=init_state,
            observers=[self],
            anti_flap_lockout_s=self._settings.anti_flap_lockout_s,
            logger=self._logger.getChild("fsm"),
        )

        # Transport and Bridge
        self._mock_mcu = mock_mcu
        if transport is not None:
            self._transport = transport
        else:
            mpu_tr, mcu_tr = MockMcuTransport.linked_pair()
            self._transport = mpu_tr
            if self._mock_mcu is None:
                self._mock_mcu = MockMcu(mcu_tr, logger=self._logger.getChild("mock_mcu"))

        self._bridge = McuBridgeWorker(
            self._transport,
            telemetry_listener=self._on_bridge_telemetry,
            link_lost_callback=self._on_bridge_link_lost,
            heartbeat_interval_s=self._settings.heartbeat_interval_s,
            hello_retry_interval_s=self._settings.hello_retry_interval_s,
            stale_threshold_ms=self._settings.stale_threshold_ms,
            lost_threshold_ms=self._settings.lost_threshold_ms,
            default_ttl_ms=self._settings.default_ttl_ms,
            logger=self._logger.getChild("bridge"),
        )

        self._last_arbiter_decision: ArbiterDecision | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def fsm_state(self) -> SystemState:
        return self._fsm.state

    @property
    def fsm(self) -> FSMEngine:
        return self._fsm

    @property
    def bridge(self) -> McuBridgeWorker:
        return self._bridge

    @property
    def bridge_health(self) -> BridgeHealth:
        return self._bridge.health

    @property
    def arbiter_stats(self) -> dict[str, Any]:
        return self._arbiter.stats()

    @property
    def mock_mcu(self) -> MockMcu | None:
        return self._mock_mcu

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start all components and optional MockMcu."""
        if self._running:
            return
        self._running = True

        if self._mock_mcu is not None:
            await self._mock_mcu.start()

        await self._bridge.start()
        self._logger.info("OrchestratorPipeline started (initial_state=%s)", self._fsm.state)

    async def stop(self) -> None:
        """Stop all components cleanly."""
        self._running = False
        await self._bridge.stop()
        if self._mock_mcu is not None:
            await self._mock_mcu.stop()
        self._logger.info("OrchestratorPipeline stopped")

    # ------------------------------------------------------------------
    # Easily Modifiable Input Interface
    # ------------------------------------------------------------------

    async def submit_event(
        self,
        trigger: str | FsmTrigger,
        origin: str = "ui",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Inject an event into the pipeline.

        External modules (ZeroMQ EventRouter, REST API, WebSockets, or test scripts)
        invoke this method to drive the pipeline.

        Args:
            trigger: FsmTrigger enum or string matching trigger token.
            origin:  Identifier of the caller ("ui", "stt", "cv", "mcu", "system").
            payload: Optional dictionary carrying metadata (e.g. setpoint_mms, confidence).

        Returns:
            Dictionary containing transition outcome, state, and arbiter decision.
        """
        payload = payload or {}
        trig_enum = FsmTrigger(trigger) if isinstance(trigger, str) else trigger

        # Special case: SET_SPEED_CMD in RUNNING does not trigger a state transition,
        # but needs Arbiter validation and Bridge dispatch.
        if trig_enum == FsmTrigger.SET_SPEED_CMD and self._fsm.state == SystemState.RUNNING:
            setpoint = payload.get("setpoint_mms", self._settings.min_speed_mms)
            decision = await self._arbiter.evaluate(Action.SET_SPEED, setpoint, origin, payload)
            self._last_arbiter_decision = decision
            if decision.permitted:
                await self._bridge.send_action(Action.SET_SPEED, setpoint)
                return {
                    "accepted": True,
                    "state": self._fsm.state.value,
                    "arbiter_decision": decision.model_dump(),
                    "detail": f"Speed set to {setpoint} mm/s",
                }
            else:
                return {
                    "accepted": False,
                    "state": self._fsm.state.value,
                    "arbiter_decision": decision.model_dump(),
                    "detail": f"Speed change blocked by arbiter: {decision.reason_name}",
                }

        # Standard transition path
        event = FsmEvent(
            trigger=trig_enum,
            origin=origin,
            payload=payload,
            timestamp=time.time(),
        )

        state_changed = await self._fsm.process(event)
        if state_changed is None:
            return {
                "accepted": False,
                "state": self._fsm.state.value,
                "arbiter_decision": None,
                "detail": f"Trigger {trig_enum.value} is not permitted from {self._fsm.state.value}",
            }

        decision_dump = (
            self._last_arbiter_decision.model_dump() if self._last_arbiter_decision else None
        )
        return {
            "accepted": True,
            "state": self._fsm.state.value,
            "previous_state": state_changed.previous.value,
            "arbiter_decision": decision_dump,
            "detail": f"Transitioned from {state_changed.previous.value} to {state_changed.current.value}",
        }

    async def feed_sensor(self, reading: SensorReading) -> None:
        """Manually override or supply sensor readings."""
        await self._sensor_source.feed(reading)

    # ------------------------------------------------------------------
    # FsmObserver Callback & Action Dispatch
    # ------------------------------------------------------------------

    async def on_state_changed(self, event: FsmStateChanged) -> None:
        """Called automatically by FSMEngine whenever state transitions."""
        self._bridge.set_fsm_state_for_heartbeat(self._fsm_state_to_int(event.current))

        # Map state transition to intended action
        action: Action | None = None
        setpoint: int | None = None

        if event.previous == SystemState.IDLE and event.current == SystemState.RUNNING:
            action = Action.START
            setpoint = event.payload.get("setpoint_mms", 100)
        elif event.previous == SystemState.PAUSED and event.current == SystemState.RUNNING:
            action = Action.RESUME
            setpoint = event.payload.get("setpoint_mms", 100)
        elif event.current == SystemState.PAUSED:
            action = Action.PAUSE
        elif event.current == SystemState.IDLE:
            if event.previous == SystemState.RECOVERING:
                action = Action.RECOVER
            else:
                action = Action.STOP
        elif event.current == SystemState.SAFETY_FAULT:
            action = Action.HALT

        if action is None:
            return

        # Evaluate safety invariants before commanding hardware
        decision = await self._arbiter.evaluate(
            action,
            setpoint,
            origin=event.origin,
            payload=event.payload,
        )
        self._last_arbiter_decision = decision

        if decision.permitted:
            await self._bridge.send_action(action, setpoint)
            self._logger.info("Dispatched action %s to MCU (decision=PERMITTED)", action)
        else:
            self._logger.warning(
                "Arbiter INHIBITED action %s: code=%s (%s)",
                action,
                decision.reason_code,
                decision.reason_name,
            )

    @staticmethod
    def _fsm_state_to_int(state: SystemState) -> int:
        mapping = {
            SystemState.IDLE: 1,
            SystemState.RUNNING: 2,
            SystemState.PAUSED: 3,
            SystemState.JAMMED: 4,
            SystemState.SAFETY_FAULT: 5,
            SystemState.RECOVERING: 6,
        }
        return mapping.get(state, 1)

    # ------------------------------------------------------------------
    # Bridge Listeners
    # ------------------------------------------------------------------

    async def _on_bridge_telemetry(self, frame: TelemetryFrame) -> None:
        """Forward telemetry from bridge into the sensor source."""
        reading = self._bridge.latest_sensor_reading()
        await self._sensor_source.feed(reading)

    async def _on_bridge_link_lost(self) -> None:
        """Trigger FSM safety fault when MCU link is lost."""
        self._logger.critical("Bridge reported link lost! Forcing SAFETY_FAULT")
        await self._fsm.process(
            FsmEvent(
                trigger=FsmTrigger.LINK_LOST,
                origin="mcu_bridge",
                payload={},
                timestamp=time.time(),
            )
        )
