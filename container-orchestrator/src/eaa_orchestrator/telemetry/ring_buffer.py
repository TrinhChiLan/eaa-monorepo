"""Bounded in-memory telemetry ring buffer.

The buffer is intentionally small and synchronous in its critical section: it
must never wait for disk I/O.  Persistence is a separate non-blocking sink.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque

from shared.contracts import PersistenceSink, SubmitOutcome
from shared.schemas.events import EventEnvelope
from shared.schemas.telemetry import RingBufferStats


class TelemetryRingBuffer:
    """A bounded, event-loop-safe store of the most recent canonical events."""

    def __init__(
        self,
        capacity: int = 1000,
        *,
        persistence: PersistenceSink | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._buffer: deque[EventEnvelope] = deque(maxlen=capacity)
        self._lock = asyncio.Lock()
        self._persistence = persistence
        self._logger = logger or logging.getLogger(__name__)

        self._total_pushed = 0
        self._evicted = 0
        self._persistence_queued = 0
        self._persistence_filtered = 0
        self._persistence_dropped = 0
        self._persistence_errors = 0

    async def push(self, event: EventEnvelope) -> None:
        """Append an event, evicting the oldest when full, then offer it to persistence."""
        if not isinstance(event, EventEnvelope):
            raise TypeError("TelemetryRingBuffer only accepts EventEnvelope instances")

        async with self._lock:
            if len(self._buffer) == self._capacity:
                self._evicted += 1
            self._buffer.append(event)
            self._total_pushed += 1

        if self._persistence is None:
            return

        try:
            outcome = await self._persistence.submit(event)
        except Exception:
            self._persistence_errors += 1
            self._logger.exception("Persistence sink rejected event %s", event.event_id)
            return

        if outcome == SubmitOutcome.QUEUED:
            self._persistence_queued += 1
        elif outcome == SubmitOutcome.FILTERED:
            self._persistence_filtered += 1
        else:
            # DROPPED and STOPPED both mean "not persisted".
            self._persistence_dropped += 1

    async def get_latest(self, count: int = 10) -> list[EventEnvelope]:
        """Return the newest ``count`` events in chronological ascending order."""
        if count <= 0:
            return []
        async with self._lock:
            items = list(self._buffer)
        return items[-count:]

    async def snapshot(self, limit: int | None = None) -> list[EventEnvelope]:
        """Return the buffer (or its newest ``limit`` entries) in ascending order."""
        if limit is not None and limit <= 0:
            return []
        async with self._lock:
            items = list(self._buffer)
        return items if limit is None else items[-limit:]

    async def clear(self) -> None:
        async with self._lock:
            self._buffer.clear()

    async def stats(self) -> RingBufferStats:
        async with self._lock:
            size = len(self._buffer)
            oldest = self._buffer[0].event_time if self._buffer else None
            newest = self._buffer[-1].event_time if self._buffer else None

        return RingBufferStats(
            capacity=self._capacity,
            size=size,
            total_pushed=self._total_pushed,
            evicted=self._evicted,
            persistence_queued=self._persistence_queued,
            persistence_filtered=self._persistence_filtered,
            persistence_dropped=self._persistence_dropped,
            persistence_errors=self._persistence_errors,
            oldest_event_time=oldest,
            newest_event_time=newest,
        )


__all__ = ["RingBufferStats", "TelemetryRingBuffer"]
