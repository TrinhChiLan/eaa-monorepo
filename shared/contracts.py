"""Cross-module interfaces.

Modules depend on these small protocols rather than on one another's concrete
classes.  This keeps tests fast and lets the router be used with an in-memory
sink, or the ring buffer with a fake persistence sink.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol

from shared.schemas.events import EventEnvelope


class SubmitOutcome(StrEnum):
    QUEUED = "queued"
    FILTERED = "filtered"
    DROPPED = "dropped"
    STOPPED = "stopped"


class EventSink(Protocol):
    """A consumer of normalized events (the ring buffer in production)."""

    async def push(self, event: EventEnvelope) -> None: ...


class PersistenceSink(Protocol):
    """A non-blocking persistence queue used by the ring buffer."""

    async def submit(self, event: EventEnvelope) -> SubmitOutcome: ...


__all__ = ["EventSink", "PersistenceSink", "SubmitOutcome"]
