"""engine.py — Table-driven FSM engine for the EAA conveyor control_pipeline.

The ``FSMEngine`` enforces deterministic, auditable state transitions for the
industrial conveyor.  It is a **pure-state** component:

- It never issues MCU commands directly.
- It validates that a trigger is structurally permitted from the current state.
- It notifies registered ``FsmObserver`` callbacks after each transition so that
  downstream components (SafetyInvariantArbiter, telemetry sinks, …) can react.
- An anti-flap lockout gate prevents rapid-fire ``START_CMD`` hammering.

Concurrency
-----------
All state mutations are serialised through ``asyncio.Lock``.  The engine is
designed for a single asyncio event-loop; it must not be shared across threads
without an external synchronisation wrapper.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from control_pipeline.schemas.fsm import (
    FsmEvent,
    FsmStateChanged,
    FsmTrigger,
    SystemState,
)
from control_pipeline.fsm.transitions import ALWAYS_PERMITTED_TRIGGERS, TRANSITION_TABLE

# ---------------------------------------------------------------------------
# Local logger (used when no logger is injected)
# ---------------------------------------------------------------------------

_MODULE_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FsmObserver protocol (inline, mirrors contracts.FsmObserver)
# ---------------------------------------------------------------------------
# We import from contracts when it is available; if contracts.py has not been
# written yet we fall back to a runtime Protocol check so the module still
# imports cleanly during early bootstrap.

try:
    from control_pipeline.contracts import FsmObserver  # type: ignore[import-untyped]
except ModuleNotFoundError:  # pragma: no cover – contracts written by peer worker
    from typing import Protocol, runtime_checkable

    @runtime_checkable
    class FsmObserver(Protocol):  # type: ignore[no-redef]
        """Minimal protocol expected by FSMEngine observers."""

        async def on_state_changed(self, change: FsmStateChanged) -> None:
            """Called after every successful FSM state transition."""
            ...


# ---------------------------------------------------------------------------
# FSMEngine
# ---------------------------------------------------------------------------


class FSMEngine:
    """Table-driven FSM for the EAA industrial conveyor control system.

    The engine validates structural eligibility of a trigger against the
    current state, applies an anti-flap gate on ``START_CMD``, updates
    internal state atomically, and notifies all registered observers.

    It never directly commands the MCU — it proposes a new state and the
    ``SafetyInvariantArbiter`` sitting downstream gates the actual command.

    Parameters
    ----------
    initial_state:
        The ``SystemState`` the engine starts in.  Defaults to ``IDLE``.
    observers:
        Zero or more objects implementing the ``FsmObserver`` protocol.
        Notifications are delivered sequentially; a crashing observer does
        **not** abort delivery to later observers.
    clock:
        Callable returning the current time as a float (Unix seconds).
        Defaults to ``time.time``; injectable for deterministic testing.
    logger:
        Optional :class:`logging.Logger`.  When ``None`` the module-level
        logger ``control_pipeline.fsm.engine`` is used.
    anti_flap_lockout_s:
        Minimum seconds that must elapse between two successive ``START_CMD``
        triggers.  Defaults to ``5.0``.
    """

    def __init__(
        self,
        initial_state: SystemState = SystemState.IDLE,
        observers: list[FsmObserver] = (),  # type: ignore[assignment]
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
        anti_flap_lockout_s: float = 5.0,
    ) -> None:
        self._current_state: SystemState = initial_state
        self._observers: list[FsmObserver] = list(observers)
        self._clock: Callable[[], float] = clock
        self._log: logging.Logger = logger or _MODULE_LOGGER
        self._anti_flap_lockout_s: float = anti_flap_lockout_s

        # Bookkeeping
        self._lock_obj: asyncio.Lock | None = None
        self._last_start_at: float | None = None
        self._last_transition_at: float | None = None
        self._transition_count: int = 0

    def _get_lock(self) -> asyncio.Lock:
        if self._lock_obj is None:
            self._lock_obj = asyncio.Lock()
        return self._lock_obj

    # ------------------------------------------------------------------
    # Public read-only property
    # ------------------------------------------------------------------

    def add_observer(self, observer: FsmObserver) -> None:
        """Register a new observer to receive state transition events."""
        if observer not in self._observers:
            self._observers.append(observer)

    @property
    def state(self) -> SystemState:
        """The current ``SystemState`` (snapshot — not locked)."""
        return self._current_state

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    async def process(self, event: FsmEvent) -> FsmStateChanged | None:
        """Process an inbound FSM event and transition state if permitted.

        Steps
        -----
        1. Look up ``(current_state, trigger)`` in the transition table.
           If the trigger is in ``ALWAYS_PERMITTED_TRIGGERS`` but has no
           explicit table entry for the current state, synthesise a
           ``→ SAFETY_FAULT`` transition.
           If neither applies, log the illegal trigger and return ``None``.
        2. Apply anti-flap gate for ``START_CMD``: if the last ``START_CMD``
           succeeded less than ``anti_flap_lockout_s`` seconds ago, log
           ``FLAP_LOCKOUT`` and return ``None``.
        3. Atomically update ``_current_state``.
        4. Notify all observers (errors are caught per-observer and logged).
        5. Return the resulting ``FsmStateChanged`` record.

        Parameters
        ----------
        event:
            The ``FsmEvent`` to process.

        Returns
        -------
        FsmStateChanged | None
            ``None`` when the transition is rejected (illegal trigger or
            anti-flap lockout); otherwise the transition record.
        """
        async with self._get_lock():
            previous = self._current_state
            trigger = event.trigger

            # 1. Resolve destination state -----------------------------------
            to_state = TRANSITION_TABLE.get((previous, trigger))

            if to_state is None:
                if trigger in ALWAYS_PERMITTED_TRIGGERS:
                    # Safety/link triggers override any state → SAFETY_FAULT
                    to_state = SystemState.SAFETY_FAULT
                    self._log.warning(
                        "FSM | always-permitted trigger %s from %s "
                        "(no explicit table entry) → %s",
                        trigger,
                        previous,
                        to_state,
                    )
                else:
                    self._log.warning(
                        "FSM | illegal trigger %s from state %s — rejected",
                        trigger,
                        previous,
                    )
                    return None

            # 2. Anti-flap gate (START_CMD only) -----------------------------
            if trigger is FsmTrigger.START_CMD:
                now = self._clock()
                if (
                    self._last_start_at is not None
                    and (now - self._last_start_at) < self._anti_flap_lockout_s
                ):
                    elapsed = now - self._last_start_at
                    self._log.warning(
                        "FSM | FLAP_LOCKOUT — START_CMD rejected; "
                        "last start was %.2fs ago (lockout=%.2fs)",
                        elapsed,
                        self._anti_flap_lockout_s,
                    )
                    return None

            # 3. Transition ---------------------------------------------------
            transition_time = self._clock()
            self._current_state = to_state
            self._transition_count += 1
            self._last_transition_at = transition_time

            if trigger is FsmTrigger.START_CMD:
                self._last_start_at = transition_time

            change = FsmStateChanged(
                previous=previous,
                current=to_state,
                trigger=trigger,
                origin=event.origin,
                at=transition_time,
                payload=event.payload,
            )

            self._log.info(
                "FSM | %s -[%s]-> %s (origin=%s)",
                previous,
                trigger,
                to_state,
                event.origin,
            )

        # 4. Notify observers (outside the lock so observers can re-enter) ---
        await self._notify_observers(change)

        # 5. Return result ----------------------------------------------------
        return change

    async def force_state(
        self,
        state: SystemState,
        reason: str = "forced",
    ) -> None:
        """Force the FSM into ``state`` without transition validation.

        This bypass is reserved for two operational scenarios:

        - **SAFETY_EVENT / BOOT re-sync**: an external watchdog or boot
          handler needs to guarantee the FSM reaches a safe state regardless
          of the current transition graph.
        - **Test / diagnostic harnesses** that need to seed a specific state.

        A synthetic ``FsmStateChanged`` is emitted to observers so that
        telemetry and downstream components stay coherent.

        Parameters
        ----------
        state:
            The target ``SystemState`` to force.
        reason:
            Human-readable explanation logged at WARNING level.
        """
        async with self._get_lock():
            previous = self._current_state
            transition_time = self._clock()

            self._current_state = state
            self._transition_count += 1
            self._last_transition_at = transition_time

            self._log.warning(
                "FSM | force_state %s → %s  reason=%r",
                previous,
                state,
                reason,
            )

            change = FsmStateChanged(
                previous=previous,
                current=state,
                trigger=FsmTrigger.BOOT,   # sentinel; caller may override via observers
                origin="system",
                at=transition_time,
            )

        await self._notify_observers(change)

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a point-in-time snapshot of engine bookkeeping.

        Returns
        -------
        dict
            Keys: ``state``, ``last_transition_at``, ``transition_count``.
        """
        return {
            "state": self._current_state.value,
            "last_transition_at": self._last_transition_at,
            "transition_count": self._transition_count,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _notify_observers(self, change: FsmStateChanged) -> None:
        """Deliver *change* to every registered observer.

        Each observer's ``on_state_changed`` is called in registration order.
        Exceptions raised by an individual observer are caught and logged so
        that a crashing observer cannot prevent delivery to later observers or
        corrupt FSM state.
        """
        for observer in self._observers:
            try:
                await observer.on_state_changed(change)
            except Exception:  # noqa: BLE001
                self._log.exception(
                    "FSM | observer %r raised during on_state_changed "
                    "(previous=%s, current=%s) — continuing",
                    observer,
                    change.previous,
                    change.current,
                )
