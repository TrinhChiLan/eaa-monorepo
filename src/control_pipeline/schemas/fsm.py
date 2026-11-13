"""fsm.py — FSM state/trigger enums and event schemas for the EAA control_pipeline.

All types in this module are self-contained; nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import enum
from typing import Any, NamedTuple

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class SystemState(str, enum.Enum):
    """Enumeration of legal FSM macro-states for the conveyor system."""

    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    JAMMED = "jammed"
    SAFETY_FAULT = "safety_fault"
    RECOVERING = "recovering"


class FsmTrigger(str, enum.Enum):
    """All recognised trigger tokens that can drive an FSM transition."""

    START_CMD = "start_cmd"
    STOP_CMD = "stop_cmd"
    PAUSE_CMD = "pause_cmd"
    RESUME_CMD = "resume_cmd"
    SET_SPEED_CMD = "set_speed_cmd"
    JAM_DETECTED = "jam_detected"
    VERIFY_CLEARANCE = "verify_clearance"
    FAULT_CLEARED = "fault_cleared"
    SAFETY_EVENT = "safety_event"
    LINK_LOST = "link_lost"
    BOOT = "boot"


class Action(str, enum.Enum):
    """Commands the arbiter may propose to the bridge command sink."""

    START = "start"
    RESUME = "resume"
    SET_SPEED = "set_speed"
    STOP = "stop"
    PAUSE = "pause"
    RECOVER = "recover"
    HALT = "halt"


# ---------------------------------------------------------------------------
# Structural / named-tuple types
# ---------------------------------------------------------------------------


class FsmTransition(NamedTuple):
    """Immutable record describing one edge in the FSM transition table.

    Attributes:
        from_state: The source ``SystemState`` before the transition.
        trigger:    The ``FsmTrigger`` that fires the transition.
        to_state:   The destination ``SystemState`` after the transition.
    """

    from_state: SystemState
    trigger: FsmTrigger
    to_state: SystemState


# ---------------------------------------------------------------------------
# Pydantic event models
# ---------------------------------------------------------------------------


class FsmEvent(BaseModel):
    """An inbound event presented to the FSM engine for processing.

    Attributes:
        trigger:   The trigger token carried by this event.
        origin:    Subsystem that generated the event.
                   One of ``"cv"``, ``"stt"``, ``"ui"``, ``"mcu"``, ``"system"``.
        payload:   Arbitrary key/value extras (e.g. ``{"setpoint_mms": 300}``
                   for a ``SET_SPEED_CMD`` trigger).
        timestamp: Unix time (seconds, float) when the event was created.
    """

    model_config = ConfigDict(frozen=True)

    trigger: FsmTrigger
    origin: str
    payload: dict[str, Any]
    timestamp: float


class FsmStateChanged(BaseModel):
    """Notification emitted by the FSM engine after every successful transition.

    Attributes:
        previous: The ``SystemState`` before the transition.
        current:  The ``SystemState`` after the transition.
        trigger:  The ``FsmTrigger`` that caused the change.
        origin:   Subsystem that raised the originating ``FsmEvent``.
        at:       Unix time (seconds, float) of the transition.
    """

    model_config = ConfigDict(frozen=True)

    previous: SystemState
    current: SystemState
    trigger: FsmTrigger
    origin: str
    at: float
    payload: dict[str, Any] = Field(default_factory=dict)
