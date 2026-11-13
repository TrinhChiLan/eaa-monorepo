"""arbiter.py – SafetyInvariantArbiter for the EAA control_pipeline.

Sits between FSMEngine and McuBridgeWorker as the LAST safety gate before any
action reaches the MCU.  Implements the "MPU Proposes, MCU Decides" contract.

Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Protocol, runtime_checkable

from control_pipeline.schemas.arbiter import ArbiterDecision, ReasonCode, SensorReading
from control_pipeline.schemas.fsm import Action

# ---------------------------------------------------------------------------
# Local SensorSource protocol (mirrors control_pipeline.contracts.SensorSource)
# ---------------------------------------------------------------------------


@runtime_checkable
class SensorSource(Protocol):
    """Async provider of the latest sensor snapshot."""

    async def latest(self) -> SensorReading:  # noqa: D102
        ...


# ---------------------------------------------------------------------------
# Action classification helpers
# ---------------------------------------------------------------------------

#: Actions that command the belt to move / change speed.
_ENERGIZING: frozenset[Action] = frozenset(
    {Action.START, Action.RESUME, Action.SET_SPEED}
)

#: Recovery action – checks I-2 strictly.
_RECOVERY: frozenset[Action] = frozenset({Action.RECOVER})

#: Stop-class actions – skip I-2 and I-3; only full link-down rejects them.
_STOP_CLASS: frozenset[Action] = frozenset({Action.STOP, Action.PAUSE})

#: Safety-override action – always permitted, no invariant run at all.
_ALWAYS_PERMIT: frozenset[Action] = frozenset({Action.HALT})

# ---------------------------------------------------------------------------
# SafetyInvariantArbiter
# ---------------------------------------------------------------------------


class SafetyInvariantArbiter:
    """Last-resort safety gate between FSMEngine and McuBridgeWorker.

    Evaluates four hardware invariants (I-1 through I-4) plus optional
    voice-confidence and speed-limit checks.  Returns an :class:`ArbiterDecision`
    for every proposed action; callers MUST honour ``permitted=False`` by not
    forwarding the action to the MCU bridge.

    Args:
        sensor_source:      Async provider of the current :class:`SensorReading`.
        max_current_ma:     Motor current ceiling in milliamps (default 2 500 mA).
        max_temp_celsius:   Motor temperature ceiling in °C (default 80.0 °C).
        min_speed_mms:      Minimum legal setpoint in mm/s (default 50).
        max_speed_mms:      Maximum legal setpoint in mm/s (default 500).
        min_stt_confidence: Minimum STT confidence score [0, 1] (default 0.80).
        max_intent_age_ms:  Maximum STT intent age in ms before expiry (default 2 000).
        logger:             Optional logger; a module-level logger is used if *None*.
    """

    def __init__(
        self,
        sensor_source: SensorSource,
        *,
        max_current_ma: float = 2500.0,
        max_temp_celsius: float = 80.0,
        min_speed_mms: int = 50,
        max_speed_mms: int = 500,
        min_stt_confidence: float = 0.80,
        max_intent_age_ms: int = 2000,
        logger: logging.Logger | None = None,
    ) -> None:
        self._source = sensor_source
        self._max_current_ma = max_current_ma
        self._max_temp_celsius = max_temp_celsius
        self._min_speed_mms = min_speed_mms
        self._max_speed_mms = max_speed_mms
        self._min_stt_confidence = min_stt_confidence
        self._max_intent_age_ms = max_intent_age_ms
        self._log = logger or logging.getLogger(__name__)

        # Counters (stats())
        self._total_evaluated: int = 0
        self._total_permitted: int = 0
        self._denied: dict[str, int] = {
            "I-1_estop": 0,
            "I-2_pe": 0,
            "I-3_current": 0,
            "I-3_temp": 0,
            "I-4_link": 0,
            "speed_limit": 0,
            "voice_confidence": 0,
            "voice_intent_age": 0,
            "fault_latched": 0,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _decide(
        self,
        action: Action,
        proposed_setpoint_mms: int | None,
        permitted: bool,
        code: ReasonCode,
    ) -> ArbiterDecision:
        """Construct and return an :class:`ArbiterDecision`."""
        return ArbiterDecision(
            permitted=permitted,
            reason_code=int(code),
            reason_name=code.name,
            proposed_action=action,
            proposed_setpoint_mms=proposed_setpoint_mms,
        )

    def _deny(
        self,
        action: Action,
        proposed_setpoint_mms: int | None,
        code: ReasonCode,
        invariant_key: str,
    ) -> ArbiterDecision:
        """Record a denial counter and return a rejected :class:`ArbiterDecision`."""
        self._denied[invariant_key] = self._denied.get(invariant_key, 0) + 1
        self._log.warning(
            "arbiter denied action=%s code=%s(%02X) invariant=%s",
            action.value,
            code.name,
            int(code),
            invariant_key,
        )
        return self._decide(action, proposed_setpoint_mms, False, code)

    def _permit(
        self,
        action: Action,
        proposed_setpoint_mms: int | None,
    ) -> ArbiterDecision:
        """Return an approved :class:`ArbiterDecision`."""
        self._total_permitted += 1
        return self._decide(action, proposed_setpoint_mms, True, ReasonCode.OK_NOMINAL)

    # ------------------------------------------------------------------
    # Individual invariant checks (all return None if OK, ArbiterDecision if denied)
    # ------------------------------------------------------------------

    def _check_i1_estop(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """I-1: E-stop circuit must be closed (safe)."""
        if sensor.estop_closed:
            return None
        return self._deny(action, setpoint, ReasonCode.ERR_ESTOP_ACTIVE, "I-1_estop")

    def _check_i2_pe(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """I-2: Photoelectric beam — only checked for RECOVER.

        START and RESUME explicitly skip this check; a stationary workpiece on
        a stopped belt is a valid condition for those commands.
        """
        # Only enforced for RECOVER
        if action not in _RECOVERY:
            return None
        if sensor.pe_clear:
            return None
        return self._deny(
            action, setpoint, ReasonCode.ERR_PHOTOELECTRIC_BLOCKED, "I-2_pe"
        )

    def _check_i3_current(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """I-3a: Motor current must not exceed the electrical ceiling."""
        if sensor.motor_current_ma <= self._max_current_ma:
            return None
        return self._deny(action, setpoint, ReasonCode.CURRENT_NOT_NOMINAL, "I-3_current")

    def _check_i3_temp(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """I-3b: Motor temperature must not exceed the thermal ceiling.

        The check is skipped when no thermal sensor is installed (signalled by
        ``infrared_temp_celsius`` being non-finite, mirroring the bridge's
        sentinel mapping of raw ``temp_c10 == 0x7FFF`` / ``0x7FFE``).
        """
        temp = sensor.infrared_temp_celsius
        if not math.isfinite(temp):
            # Thermal sensor not installed or reporting a fault — skip check.
            return None
        if temp <= self._max_temp_celsius:
            return None
        return self._deny(action, setpoint, ReasonCode.ERR_MOTOR_OVERTEMP, "I-3_temp")

    def _check_i4_link(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
        is_energizing: bool,
        is_stop_class: bool,
    ) -> ArbiterDecision | None:
        """I-4: Serial link must be healthy enough for the requested action.

        Rules:
        - ``"UP"``  → always OK.
        - ``"STALE"`` → reject energizing actions; allow stop-class actions.
        - ``"DOWN"``/``"SYNC"``/``"LOST"`` → reject everything except HALT
          (HALT is handled before this call and never reaches here).
        - ``hb_valid`` must be ``True`` for energizing actions.
        """
        link = sensor.link_state

        if link in ("DOWN", "SYNC", "LOST"):
            # Hard link failure – only HALT passes (already returned above).
            return self._deny(action, setpoint, ReasonCode.LINK_DOWN, "I-4_link")

        if link == "STALE":
            if is_energizing or action in _RECOVERY:
                return self._deny(action, setpoint, ReasonCode.LINK_STALE, "I-4_link")
            # STOP / PAUSE are permitted through a stale link.
            return None

        # link == "UP"
        if is_energizing and not sensor.hb_valid:
            return self._deny(action, setpoint, ReasonCode.LINK_DOWN, "I-4_link")

        return None

    def _check_speed(
        self,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """Speed-limit check for SET_SPEED (and any action with a setpoint)."""
        if action not in (Action.SET_SPEED,):
            # Only enforce for explicit speed-setting commands.
            return None
        if setpoint is None:
            # Missing setpoint for SET_SPEED is a bad parameter.
            return self._deny(action, setpoint, ReasonCode.ERR_BAD_PARAM, "speed_limit")
        if self._min_speed_mms <= setpoint <= self._max_speed_mms:
            return None
        return self._deny(action, setpoint, ReasonCode.ERR_BAD_PARAM, "speed_limit")

    def _check_voice(
        self,
        action: Action,
        setpoint: int | None,
        origin: str,
        payload: dict[str, Any],
    ) -> ArbiterDecision | None:
        """Voice-confidence gate (applied only when origin == "stt").

        Checks:
        1. ``intent_age_ms`` – if present and older than ``max_intent_age_ms``,
           reject with ``INTENT_EXPIRED``.
        2. ``confidence`` – if present and below ``min_stt_confidence``,
           reject with ``LOW_CONFIDENCE``.
        """
        if origin != "stt":
            return None

        intent_age_ms: int | None = payload.get("intent_age_ms")
        if intent_age_ms is not None and intent_age_ms > self._max_intent_age_ms:
            return self._deny(
                action, setpoint, ReasonCode.INTENT_EXPIRED, "voice_intent_age"
            )

        confidence: float | None = payload.get("confidence")
        if confidence is not None and confidence < self._min_stt_confidence:
            return self._deny(
                action, setpoint, ReasonCode.LOW_CONFIDENCE, "voice_confidence"
            )

        return None

    def _check_fault_latched(
        self,
        sensor: SensorReading,
        action: Action,
        setpoint: int | None,
    ) -> ArbiterDecision | None:
        """Fault-latched gate: energizing actions blocked while latch is active."""
        if not sensor.fault_latched:
            return None
        return self._deny(
            action, setpoint, ReasonCode.ERR_FAULT_LATCHED, "fault_latched"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def evaluate(
        self,
        action: Action,
        proposed_setpoint_mms: int | None = None,
        origin: str = "system",
        payload: dict[str, Any] | None = None,
        *,
        setpoint_mms: int | None = None,
    ) -> ArbiterDecision:
        if proposed_setpoint_mms is None and setpoint_mms is not None:
            proposed_setpoint_mms = setpoint_mms
        if payload is None:
            payload = {}
        """Fetch the latest sensor snapshot and run all applicable invariants.

        Invariants are evaluated in strict order; the first failure is returned
        immediately (early-exit, no nesting).

        Action categories and applicable checks:

        ============  ===================================================
        Category      Checks applied
        ============  ===================================================
        HALT          None — always permitted.
        STOP / PAUSE  I-4 (link DOWN/LOST only); skip I-1, I-2, I-3.
        ENERGIZING    Voice → Fault-latched → I-1 → I-2 (skipped) →
                      I-3 → I-4 → Speed.
        RECOVER       Voice → Fault-latched → I-1 → I-2 (strict) →
                      I-3 → I-4.
        ============  ===================================================

        Args:
            action:                The proposed :class:`Action`.
            proposed_setpoint_mms: Speed setpoint in mm/s, or ``None``.
            origin:                Subsystem that raised the command
                                   (``"cv"``, ``"stt"``, ``"ui"``, …).
            payload:               Arbitrary extras from the originating event.

        Returns:
            :class:`ArbiterDecision` with ``permitted=True`` and
            ``reason_code=OK_NOMINAL`` if all checks pass, or
            ``permitted=False`` with the first failing ``reason_code``.
        """
        self._total_evaluated += 1
        s = proposed_setpoint_mms  # local alias for brevity

        # ── HALT: unconditionally permitted ────────────────────────────
        if action in _ALWAYS_PERMIT:
            self._log.debug("arbiter: HALT always permitted")
            return self._permit(action, s)

        # ── Fetch sensor snapshot ──────────────────────────────────────
        sensor: SensorReading = await self._source.latest()

        is_energizing = action in _ENERGIZING
        is_stop_class = action in _STOP_CLASS

        # ── STOP / PAUSE: only hard link failure can block ─────────────
        if is_stop_class:
            link = sensor.link_state
            if link in ("DOWN", "SYNC", "LOST"):
                return self._deny(action, s, ReasonCode.LINK_DOWN, "I-4_link")
            return self._permit(action, s)

        # ── All remaining actions (energizing + RECOVER) ───────────────

        # Voice confidence (pre-hardware; cheap, no sensor needed)
        if (d := self._check_voice(action, s, origin, payload)) is not None:
            return d

        # Fault-latched gate (energizing + recover)
        if (d := self._check_fault_latched(sensor, action, s)) is not None:
            return d

        # I-1: E-stop
        if (d := self._check_i1_estop(sensor, action, s)) is not None:
            return d

        # I-2: Photoelectric (RECOVER only; energizing skips)
        if (d := self._check_i2_pe(sensor, action, s)) is not None:
            return d

        # I-3: Motor electrical ceiling (energizing + recover)
        if (d := self._check_i3_current(sensor, action, s)) is not None:
            return d
        if (d := self._check_i3_temp(sensor, action, s)) is not None:
            return d

        # I-4: Link staleness
        if (
            d := self._check_i4_link(sensor, action, s, is_energizing, is_stop_class)
        ) is not None:
            return d

        # Speed limits (SET_SPEED only)
        if (d := self._check_speed(action, s)) is not None:
            return d

        # All invariants passed.
        return self._permit(action, s)

    def stats(self) -> dict[str, int]:
        """Return evaluation counters for observability and testing.

        Returns:
            A dictionary with keys:

            - ``total_evaluated`` – total calls to :meth:`evaluate`.
            - ``permitted`` – decisions with ``permitted=True``.
            - ``denied_I-1_estop`` through ``denied_fault_latched`` – per-invariant
              denial counts.
        """
        result: dict[str, int] = {
            "total_evaluated": self._total_evaluated,
            "permitted": self._total_permitted,
        }
        for key, count in self._denied.items():
            result[f"denied_{key}"] = count
        return result
