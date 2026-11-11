"""Observability and retention data structures shared by modules and tests."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """SQLite retention policy.

    ``max_rows`` keeps the newest rows by insertion id.  ``max_age_seconds``
    deletes rows whose source event time is older than the cutoff.  Either or
    both may be set; both unset means "keep everything".
    """

    max_rows: int | None = None
    max_age_seconds: float | None = None
    prune_interval_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_rows is not None and self.max_rows <= 0:
            raise ValueError("max_rows must be positive when set")
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive when set")
        if self.prune_interval_seconds <= 0:
            raise ValueError("prune_interval_seconds must be positive")

    @property
    def enabled(self) -> bool:
        return self.max_rows is not None or self.max_age_seconds is not None


@dataclass(frozen=True)
class RingBufferStats:
    capacity: int
    size: int
    total_pushed: int
    evicted: int
    persistence_queued: int
    persistence_filtered: int
    persistence_dropped: int
    persistence_errors: int
    oldest_event_time: float | None
    newest_event_time: float | None


@dataclass(frozen=True)
class PersistenceStats:
    queue_capacity: int
    queue_size: int
    submitted: int
    queued: int
    filtered: int
    dropped_queue_full: int
    stopped_rejected: int
    batches_flushed: int
    rows_inserted: int
    failed_batches: int
    pruned_rows: int
    unflushed_on_shutdown: int
    last_flush_at: float | None
    running: bool


@dataclass(frozen=True)
class RouterStats:
    received: int
    emitted: int
    invalid: int
    oversized: int
    unknown_topic: int
    handler_errors: int
    socket_errors: int
    sink_errors: int
    last_event_at: float | None


__all__ = [
    "PersistenceStats",
    "RetentionPolicy",
    "RingBufferStats",
    "RouterStats",
]
