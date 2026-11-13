"""contracts.py — Abstract Protocol interfaces for the EAA control_pipeline.

Each ``Protocol`` defines the async boundary between the three main modules
(FSMEngine, SafetyInvariantArbiter, McuBridgeWorker) so that they can be
developed, tested, and replaced independently.

Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from control_pipeline.schemas.arbiter import SensorReading
from control_pipeline.schemas.bridge import McuEventFrame, TelemetryFrame
from control_pipeline.schemas.fsm import Action, FsmStateChanged


@runtime_checkable
class SensorSource(Protocol):
    """Supplier of the most-recent sensor snapshot.

    Implementations are typically the ``McuBridgeWorker``, which derives a
    :class:`~control_pipeline.schemas.arbiter.SensorReading` from the latest
    decoded :class:`~control_pipeline.schemas.bridge.TelemetryFrame`.
    """

    async def latest(self) -> SensorReading:
        """Return the most-recent :class:`~control_pipeline.schemas.arbiter.SensorReading`.

        Implementations MUST NOT block; if no frame has been received yet they
        should return a safe-defaults reading with ``sensors_ok=False``.
        """
        ...


@runtime_checkable
class FsmObserver(Protocol):
    """Listener notified whenever the FSM engine completes a state transition.

    Implementations may update UI state, log events, or trigger downstream
    safety re-evaluations.
    """

    async def on_state_changed(self, event: FsmStateChanged) -> None:
        """Handle a completed FSM state transition.

        Args:
            event: Immutable record describing the transition.
        """
        ...


@runtime_checkable
class BridgeCommandSink(Protocol):
    """Receives :class:`~control_pipeline.schemas.fsm.Action` decisions from the
    arbiter and serialises them into B-03 binary frames for transmission to the
    MCU.
    """

    async def send_action(
        self,
        action: Action,
        setpoint_mms: int | None,
        cmd_id: int,
        ttl_ms: int,
    ) -> None:
        """Encode and transmit an action command to the MCU.

        Args:
            action:          The :class:`~control_pipeline.schemas.fsm.Action` to
                             execute (e.g. ``Action.START``).
            setpoint_mms:    Speed setpoint in mm/s, or ``None`` when the action
                             does not carry a speed parameter (e.g. STOP, HALT).
            cmd_id:          Monotonically-increasing command identifier used to
                             correlate :class:`~control_pipeline.schemas.bridge.CmdAckFrame`
                             responses.
            ttl_ms:          Time-to-live in ms; the MCU MUST reject the command
                             if it cannot execute within this window.
        """
        ...


@runtime_checkable
class TelemetryListener(Protocol):
    """Consumer of decoded telemetry frames delivered by the bridge worker."""

    async def on_telemetry(self, frame: TelemetryFrame) -> None:
        """Process an incoming telemetry frame.

        Args:
            frame: The decoded :class:`~control_pipeline.schemas.bridge.TelemetryFrame`.
        """
        ...


@runtime_checkable
class McuEventListener(Protocol):
    """Consumer of unsolicited MCU event frames delivered by the bridge worker."""

    async def on_mcu_event(self, event: McuEventFrame) -> None:
        """Process an unsolicited MCU event.

        Args:
            event: The decoded :class:`~control_pipeline.schemas.bridge.McuEventFrame`.
        """
        ...
