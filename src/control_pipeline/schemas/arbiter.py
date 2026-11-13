"""arbiter.py — SafetyInvariantArbiter schemas for the EAA control_pipeline.

Defines sensor readings, arbiter decisions, and the unified B-03 reason-code
enumeration.  Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from control_pipeline.schemas.fsm import Action


# ---------------------------------------------------------------------------
# B-03 unified reason-code table
# ---------------------------------------------------------------------------


class ReasonCode(enum.IntEnum):
    """Unified 8-bit reason codes from the B-03 v1.0 specification.

    MCU-originated codes occupy 0x00–0x0F.
    MPU-originated codes occupy 0x80–0x8B.
    """

    # ------------------------------------------------------------------
    # MCU range 0x00 – 0x0F
    # ------------------------------------------------------------------
    OK_NOMINAL = 0x00
    """No fault; nominal operating condition."""

    ERR_ESTOP_ACTIVE = 0x01
    """Emergency-stop circuit is open (active)."""

    ERR_PHOTOELECTRIC_BLOCKED = 0x02
    """Photoelectric/presence sensor path is blocked."""

    ERR_MOTOR_OVERCURRENT = 0x03
    """Motor draw exceeded the over-current threshold."""

    ERR_MOTOR_OVERTEMP = 0x04
    """Motor or driver exceeded the over-temperature threshold."""

    ERR_HEARTBEAT_TIMEOUT = 0x05
    """MPU heartbeat was not received within the watchdog window."""

    ERR_STATE_NOT_PERMITTED = 0x06
    """Command rejected: not legal in the current MCU state."""

    ERR_SENSOR_LOSS = 0x07
    """One or more required sensors are unresponsive."""

    ERR_BAD_PARAM = 0x08
    """Command parameter out of range or malformed."""

    ERR_EXPIRED = 0x09
    """Command TTL elapsed before execution."""

    ERR_BUSY = 0x0A
    """MCU is processing another command; try again later."""

    ERR_UNKNOWN_CMD = 0x0B
    """Unrecognised command type received."""

    ERR_LIMIT_EXCEEDED = 0x0C
    """A physical or software limit was exceeded."""

    ERR_MPU_HALT_REQUEST = 0x0D
    """MCU acknowledged an MPU-initiated halt."""

    ERR_FAULT_LATCHED = 0x0E
    """A latched fault must be cleared before resuming."""

    ERR_NOT_SYNCED = 0x0F
    """Link handshake (HELLO/HELLO_ACK) has not completed."""

    # ------------------------------------------------------------------
    # MPU range 0x80 – 0x8B
    # ------------------------------------------------------------------
    LINK_DOWN = 0x80
    """Serial link to MCU is down; no frames received."""

    LINK_STALE = 0x81
    """Link is up but no fresh frame arrived within the stale threshold."""

    STALE_SENSORS = 0x82
    """Sensor readings are older than the staleness deadline."""

    ORIGIN_NOT_ALLOWED = 0x83
    """The requesting subsystem is not authorised for this action."""

    LOW_CONFIDENCE = 0x84
    """Computer-vision or STT confidence is below the minimum threshold."""

    INTENT_EXPIRED = 0x85
    """Intent/command window expired before it could be executed."""

    FSM_ILLEGAL = 0x86
    """Proposed action is not permitted from the current FSM state."""

    FLAP_LOCKOUT = 0x87
    """Too many rapid state transitions; flap-lockout is active."""

    DUPLICATE_REQUEST = 0x88
    """Identical request is already pending."""

    BUSY_PENDING = 0x89
    """A prior command is still in-flight; new command rejected."""

    CURRENT_NOT_NOMINAL = 0x8A
    """Motor current is outside the nominal band for safe operation."""

    NEEDS_CONFIRMATION = 0x8B
    """Action requires explicit operator confirmation before dispatch."""


# ---------------------------------------------------------------------------
# Sensor snapshot
# ---------------------------------------------------------------------------

_LinkStateLiteral = Literal["DOWN", "SYNC", "UP", "STALE", "LOST"]


class SensorReading(BaseModel):
    """Point-in-time snapshot of all sensor inputs visible to the arbiter.

    Attributes:
        estop_closed:        ``True`` when the e-stop circuit is closed (safe).
        pe_clear:            ``True`` when the photoelectric sensor path is clear.
        hb_valid:            ``True`` when the MCU heartbeat is current.
        fault_latched:       ``True`` when the MCU has a latched fault pending clearance.
        motor_enabled:       ``True`` when the motor driver output is enabled.
        sensors_ok:          ``True`` when all mandatory sensors report healthy.
        motor_current_ma:    Motor phase current in milliamps (≥ 0).
        conveyor_speed_mms:  Measured belt speed in mm/s (≥ 0).
        infrared_temp_celsius: Infrared surface temperature in °C.
        sensor_timestamp_ms: MCU tick (ms) at which sensors were sampled, or
                             ``None`` if unavailable.
        link_state:          Current serial-link state.
                             One of ``"DOWN"``, ``"SYNC"``, ``"UP"``,
                             ``"STALE"``, ``"LOST"``.
        last_frame_age_ms:   Age of the last valid telemetry frame in ms (≥ 0).
    """

    model_config = ConfigDict(frozen=True)

    estop_closed: bool
    pe_clear: bool
    hb_valid: bool
    fault_latched: bool
    motor_enabled: bool
    sensors_ok: bool
    motor_current_ma: float = Field(..., ge=0)
    conveyor_speed_mms: float = Field(..., ge=0)
    infrared_temp_celsius: float
    sensor_timestamp_ms: int | None
    link_state: _LinkStateLiteral
    last_frame_age_ms: float = Field(..., ge=0)

    @field_validator("link_state", mode="before")
    @classmethod
    def _normalize_link_state(cls, v: Any) -> str:
        if isinstance(v, str):
            return v.upper()
        if hasattr(v, "value") and isinstance(v.value, str):
            return v.value.upper()
        return v


# ---------------------------------------------------------------------------
# Arbiter decision
# ---------------------------------------------------------------------------


class ArbiterDecision(BaseModel):
    """Decision record produced by ``SafetyInvariantArbiter`` for each evaluation.

    Attributes:
        permitted:            ``True`` when the proposed action may be dispatched.
        reason_code:          8-bit unified reason code (see :class:`ReasonCode`).
        reason_name:          Human-readable name for ``reason_code``.
        proposed_action:      The :class:`~control_pipeline.schemas.fsm.Action` the
                              arbiter recommends sending to the MCU.
        proposed_setpoint_mms: Speed setpoint in mm/s, or ``None`` when not
                              applicable (e.g. for STOP/HALT).
    """

    model_config = ConfigDict(frozen=True)

    permitted: bool
    reason_code: int
    reason_name: str
    proposed_action: Action
    proposed_setpoint_mms: int | None
