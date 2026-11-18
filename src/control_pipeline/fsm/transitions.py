"""transitions.py — Static FSM transition table for the EAA conveyor control_pipeline.

Defines the full ``TRANSITION_TABLE`` mapping ``(SystemState, FsmTrigger)`` pairs
to their destination ``SystemState``, plus the set of always-permitted triggers
that override any state-specific table lookup.

Design rules
------------
- BOOT from **any** state re-syncs to IDLE — the conveyor never jumps straight
  to RUNNING after a reboot.
- SAFETY_EVENT and LINK_LOST from **any** state go to SAFETY_FAULT (enforced
  via ``ALWAYS_PERMITTED_TRIGGERS`` + engine logic).
- SET_SPEED_CMD in RUNNING does **not** change macro-state; the engine uses the
  ``SystemState.RUNNING → SystemState.RUNNING`` self-transition to validate the
  trigger is legal and to notify observers with the setpoint payload.
- RECOVERING → IDLE only (never directly back to RUNNING).
"""

from __future__ import annotations

from control_pipeline.schemas.fsm import FsmTrigger, SystemState

# ---------------------------------------------------------------------------
# Canonical transition table
# ---------------------------------------------------------------------------

TRANSITION_TABLE: dict[tuple[SystemState, FsmTrigger], SystemState] = {
    # --- IDLE -----------------------------------------------------------------
    (SystemState.IDLE, FsmTrigger.START_CMD):       SystemState.RUNNING,
    (SystemState.IDLE, FsmTrigger.BOOT):            SystemState.IDLE,
    # Safety / link overrides duplicated into every state below too,
    # but are also handled by ALWAYS_PERMITTED_TRIGGERS in the engine.
    (SystemState.IDLE, FsmTrigger.SAFETY_EVENT):    SystemState.SAFETY_FAULT,
    (SystemState.IDLE, FsmTrigger.LINK_LOST):       SystemState.SAFETY_FAULT,

    # --- RUNNING --------------------------------------------------------------
    (SystemState.RUNNING, FsmTrigger.STOP_CMD):     SystemState.IDLE,
    (SystemState.RUNNING, FsmTrigger.MCU_VOICE_STOP): SystemState.IDLE,
    (SystemState.RUNNING, FsmTrigger.PAUSE_CMD):    SystemState.PAUSED,
    # SET_SPEED_CMD: self-transition — state unchanged; setpoint passed via payload.
    (SystemState.RUNNING, FsmTrigger.SET_SPEED_CMD): SystemState.RUNNING,
    (SystemState.RUNNING, FsmTrigger.JAM_DETECTED): SystemState.JAMMED,
    (SystemState.RUNNING, FsmTrigger.SAFETY_EVENT): SystemState.SAFETY_FAULT,
    (SystemState.RUNNING, FsmTrigger.LINK_LOST):    SystemState.SAFETY_FAULT,
    (SystemState.RUNNING, FsmTrigger.BOOT):         SystemState.IDLE,

    # --- PAUSED ---------------------------------------------------------------
    (SystemState.PAUSED, FsmTrigger.RESUME_CMD):    SystemState.RUNNING,
    (SystemState.PAUSED, FsmTrigger.STOP_CMD):      SystemState.IDLE,
    (SystemState.PAUSED, FsmTrigger.MCU_VOICE_STOP): SystemState.IDLE,
    (SystemState.PAUSED, FsmTrigger.SAFETY_EVENT):  SystemState.SAFETY_FAULT,
    (SystemState.PAUSED, FsmTrigger.LINK_LOST):     SystemState.SAFETY_FAULT,
    (SystemState.PAUSED, FsmTrigger.BOOT):          SystemState.IDLE,

    # --- JAMMED ---------------------------------------------------------------
    (SystemState.JAMMED, FsmTrigger.VERIFY_CLEARANCE): SystemState.RECOVERING,
    (SystemState.JAMMED, FsmTrigger.SAFETY_EVENT):  SystemState.SAFETY_FAULT,
    (SystemState.JAMMED, FsmTrigger.LINK_LOST):     SystemState.SAFETY_FAULT,
    (SystemState.JAMMED, FsmTrigger.BOOT):          SystemState.IDLE,

    # --- RECOVERING -----------------------------------------------------------
    # NEVER goes directly back to RUNNING — always via IDLE first.
    (SystemState.RECOVERING, FsmTrigger.FAULT_CLEARED): SystemState.IDLE,
    (SystemState.RECOVERING, FsmTrigger.SAFETY_EVENT):  SystemState.SAFETY_FAULT,
    (SystemState.RECOVERING, FsmTrigger.LINK_LOST):     SystemState.SAFETY_FAULT,
    (SystemState.RECOVERING, FsmTrigger.BOOT):          SystemState.IDLE,

    # --- SAFETY_FAULT ---------------------------------------------------------
    # Only FAULT_CLEARED (after MCU confirms) can exit this state.
    (SystemState.SAFETY_FAULT, FsmTrigger.FAULT_CLEARED): SystemState.IDLE,
    (SystemState.SAFETY_FAULT, FsmTrigger.SAFETY_EVENT):  SystemState.SAFETY_FAULT,
    (SystemState.SAFETY_FAULT, FsmTrigger.LINK_LOST):     SystemState.SAFETY_FAULT,
    (SystemState.SAFETY_FAULT, FsmTrigger.BOOT):          SystemState.IDLE,
}

# ---------------------------------------------------------------------------
# Always-permitted triggers
# ---------------------------------------------------------------------------

ALWAYS_PERMITTED_TRIGGERS: frozenset[FsmTrigger] = frozenset(
    {
        FsmTrigger.SAFETY_EVENT,
        FsmTrigger.LINK_LOST,
    }
)
"""Triggers that are valid from **any** FSM state.

The engine uses this set to handle cases where a state does not have an
explicit entry in ``TRANSITION_TABLE`` for the arriving trigger — if the
trigger is in ``ALWAYS_PERMITTED_TRIGGERS`` the engine synthesises the
transition to ``SAFETY_FAULT`` rather than rejecting it as illegal.
"""
