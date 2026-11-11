"""SQLite repository and the half-sync/half-async persistence worker.

The repository performs synchronous-in-SQL terms of work on an aiosqlite
connection.  The worker owns a bounded in-memory queue and batches writes so
that the event loop and the router never wait for disk I/O.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Sequence

from eaa_orchestrator.persistence.database import Database
from shared.contracts import SubmitOutcome
from shared.schemas.events import EventEnvelope, EventSource, EventType
from shared.schemas.telemetry import PersistenceStats, RetentionPolicy

_INSERT_SQL = """
INSERT OR IGNORE INTO telemetry_events (
    event_id, schema_version, event_type, source, topic,
    sequence, event_time, ingested_at, payload_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_SELECT_COLUMNS = """
    schema_version, event_id, event_type, source, topic,
    sequence, event_time, ingested_at, payload_json
"""

_STOP = object()


class TelemetryRepository:
    """All SQL for the ``telemetry_events`` table."""

    def __init__(self, database: Database) -> None:
        self._database = database
        self._write_lock = asyncio.Lock()
        self._logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        await self._database.connect()

    async def bulk_insert(self, events: Sequence[EventEnvelope]) -> int:
        if not events:
            return 0

        rows = [
            (
                event.event_id,
                event.schema_version,
                event.event_type.value,
                event.source.value,
                event.topic,
                event.sequence,
                event.event_time,
                event.ingested_at,
                event.payload.model_dump_json(),
            )
            for event in events
        ]

        connection = self._database.connection
        async with self._write_lock:
            before = connection.total_changes
            await connection.execute("BEGIN IMMEDIATE")
            try:
                await connection.executemany(_INSERT_SQL, rows)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            return connection.total_changes - before

    async def fetch_recent(
        self,
        limit: int = 100,
        *,
        event_type: EventType | None = None,
        source: EventSource | None = None,
        since: float | None = None,
        until: float | None = None,
    ) -> list[EventEnvelope]:
        if limit <= 0:
            return []

        clauses: list[str] = []
        params: list[object] = []
        if event_type is not None:
            clauses.append("event_type = ?")
            params.append(event_type.value)
        if source is not None:
            clauses.append("source = ?")
            params.append(source.value)
        if since is not None:
            clauses.append("event_time >= ?")
            params.append(since)
        if until is not None:
            clauses.append("event_time <= ?")
            params.append(until)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT {_SELECT_COLUMNS}
            FROM telemetry_events
            {where}
            ORDER BY event_time DESC, id DESC
            LIMIT ?
        """
        params.append(limit)

        cursor = await self._database.connection.execute(sql, params)
        try:
            rows = await cursor.fetchall()
        finally:
            await cursor.close()

        # DB query is newest-first for efficient LIMIT; the public contract is
        # chronological ascending, matching the ring buffer.
        return [self._row_to_envelope(row) for row in reversed(rows)]

    async def count(self, *, event_type: EventType | None = None) -> int:
        if event_type is None:
            cursor = await self._database.connection.execute(
                "SELECT COUNT(*) FROM telemetry_events"
            )
        else:
            cursor = await self._database.connection.execute(
                "SELECT COUNT(*) FROM telemetry_events WHERE event_type = ?",
                (event_type.value,),
            )
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        return int(row[0]) if row else 0

    async def prune(
        self,
        *,
        max_rows: int | None = None,
        max_age_seconds: float | None = None,
        now: float | None = None,
    ) -> int:
        if max_rows is None and max_age_seconds is None:
            return 0
        if max_rows is not None and max_rows <= 0:
            raise ValueError("max_rows must be positive when set")
        if max_age_seconds is not None and max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive when set")

        cutoff = (time.time() if now is None else now) - (
            max_age_seconds if max_age_seconds is not None else 0.0
        )

        connection = self._database.connection
        async with self._write_lock:
            before = connection.total_changes
            await connection.execute("BEGIN IMMEDIATE")
            try:
                if max_age_seconds is not None:
                    await connection.execute(
                        "DELETE FROM telemetry_events WHERE event_time < ?", (cutoff,)
                    )
                if max_rows is not None:
                    await connection.execute(
                        """
                        DELETE FROM telemetry_events
                        WHERE id NOT IN (
                            SELECT id FROM telemetry_events
                            ORDER BY id DESC
                            LIMIT ?
                        )
                        """,
                        (max_rows,),
                    )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            return connection.total_changes - before

    async def health_check(self) -> bool:
        return await self._database.health_check()

    async def close(self) -> None:
        await self._database.close()

    @staticmethod
    def _row_to_envelope(row: Sequence[object]) -> EventEnvelope:
        return EventEnvelope(
            schema_version=int(row[0]),  # type: ignore[arg-type]
            event_id=str(row[1]),
            event_type=EventType(str(row[2])),
            source=EventSource(str(row[3])),
            topic=str(row[4]),
            sequence=int(row[5]),  # type: ignore[arg-type]
            event_time=float(row[6]),  # type: ignore[arg-type]
            ingested_at=float(row[7]),  # type: ignore[arg-type]
            payload=json.loads(str(row[8])),
        )


class HalfSyncHalfAsyncPersistenceWorker:
    """Bounded, batched, drop-oldest-on-pressure persistence queue."""

    def __init__(
        self,
        repository: TelemetryRepository,
        *,
        queue_capacity: int = 10_000,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_retries: int = 3,
        retry_backoff: float = 0.25,
        persist_event_types: frozenset[EventType] | None = None,
        retention: RetentionPolicy | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if queue_capacity <= 0:
            raise ValueError("queue_capacity must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if flush_interval <= 0:
            raise ValueError("flush_interval must be positive")
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")
        if retry_backoff < 0:
            raise ValueError("retry_backoff cannot be negative")

        self._repository = repository
        self._queue_capacity = queue_capacity
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_retries = max_retries
        self._retry_backoff = retry_backoff
        self._persist_event_types = persist_event_types
        self._retention = retention if retention is not None and retention.enabled else None
        self._logger = logger or logging.getLogger(__name__)

        self._queue: asyncio.Queue[EventEnvelope | object] = asyncio.Queue(maxsize=queue_capacity)
        self._task: asyncio.Task[None] | None = None
        self._accepting = False

        self._submitted = 0
        self._queued = 0
        self._filtered = 0
        self._dropped_queue_full = 0
        self._stopped_rejected = 0
        self._batches_flushed = 0
        self._rows_inserted = 0
        self._failed_batches = 0
        self._pruned_rows = 0
        self._unflushed_on_shutdown = 0
        self._last_flush_at: float | None = None
        self._last_prune_at: float | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._accepting = True
        self._task = asyncio.create_task(self._run(), name="eaa-persistence-worker")

    async def submit(self, event: EventEnvelope) -> SubmitOutcome:
        self._submitted += 1

        if not self._accepting:
            self._stopped_rejected += 1
            return SubmitOutcome.STOPPED

        if (
            self._persist_event_types is not None
            and event.event_type not in self._persist_event_types
        ):
            self._filtered += 1
            return SubmitOutcome.FILTERED

        if self._queue.full():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self._dropped_queue_full += 1
            except asyncio.QueueEmpty:  # pragma: no cover - single event loop
                pass

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:  # pragma: no cover - single event loop
            self._dropped_queue_full += 1
            return SubmitOutcome.DROPPED

        self._queued += 1
        return SubmitOutcome.QUEUED

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        if drain_timeout <= 0:
            raise ValueError("drain_timeout must be positive")

        self._accepting = False
        task = self._task
        if task is None:
            return

        if not task.done():
            try:
                # Prefer a graceful sentinel; only drop if the worker is stuck
                # for the whole drain budget.
                await asyncio.wait_for(self._queue.put(_STOP), timeout=drain_timeout)
            except TimeoutError:
                while self._queue.full():
                    try:
                        self._queue.get_nowait()
                        self._queue.task_done()
                        self._dropped_queue_full += 1
                    except asyncio.QueueEmpty:  # pragma: no cover
                        break
                self._queue.put_nowait(_STOP)

        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=drain_timeout)
        except TimeoutError:
            self._unflushed_on_shutdown = self._queue.qsize()
            self._logger.error(
                "Persistence worker did not drain within %.2fs; cancelling with %d queued",
                drain_timeout,
                self._unflushed_on_shutdown,
            )
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception:
            self._logger.exception("Persistence worker task failed during shutdown")
        finally:
            self._task = None
            self._accepting = False

    def stats(self) -> PersistenceStats:
        running = self._task is not None and not self._task.done()
        return PersistenceStats(
            queue_capacity=self._queue_capacity,
            queue_size=self._queue.qsize(),
            submitted=self._submitted,
            queued=self._queued,
            filtered=self._filtered,
            dropped_queue_full=self._dropped_queue_full,
            stopped_rejected=self._stopped_rejected,
            batches_flushed=self._batches_flushed,
            rows_inserted=self._rows_inserted,
            failed_batches=self._failed_batches,
            pruned_rows=self._pruned_rows,
            unflushed_on_shutdown=self._unflushed_on_shutdown,
            last_flush_at=self._last_flush_at,
            running=running,
        )

    # ------------------------------------------------------------------
    # Worker internals
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        try:
            while True:
                batch, stop_requested = await self._collect_batch()
                if batch:
                    try:
                        await self._persist_batch(batch)
                    finally:
                        for _ in batch:
                            self._queue.task_done()
                if stop_requested:
                    await self._drain_remaining()
                    break
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception("Persistence worker crashed; queued data may be lost")
            raise

    async def _collect_batch(self) -> tuple[list[EventEnvelope], bool]:
        batch: list[EventEnvelope] = []
        deadline: float | None = None
        stop_requested = False

        while len(batch) < self._batch_size:
            if deadline is None:
                timeout = self._flush_interval
            else:
                timeout = deadline - asyncio.get_running_loop().time()
            if timeout <= 0:
                break
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            except TimeoutError:
                break

            if item is _STOP:
                self._queue.task_done()
                stop_requested = True
                break

            assert isinstance(item, EventEnvelope)  # queue only accepts envelopes
            if deadline is None:
                # The flush interval starts when the first item of a batch arrives.
                deadline = asyncio.get_running_loop().time() + self._flush_interval
            batch.append(item)

        return batch, stop_requested

    async def _drain_remaining(self) -> None:
        while not self._queue.empty():
            batch: list[EventEnvelope] = []
            while len(batch) < self._batch_size:
                try:
                    item = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is _STOP:
                    self._queue.task_done()
                    continue
                assert isinstance(item, EventEnvelope)
                batch.append(item)
            if not batch:
                break
            try:
                await self._persist_batch(batch)
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def _persist_batch(self, batch: list[EventEnvelope]) -> None:
        for attempt in range(self._max_retries + 1):
            try:
                inserted = await self._repository.bulk_insert(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt >= self._max_retries:
                    self._failed_batches += 1
                    self._logger.exception(
                        "Dropping batch of %d events after %d attempts",
                        len(batch),
                        attempt + 1,
                    )
                    return
                delay = min(self._retry_backoff * (2**attempt), 5.0)
                self._logger.warning(
                    "Telemetry batch write failed (attempt %d/%d); retrying in %.2fs",
                    attempt + 1,
                    self._max_retries + 1,
                    delay,
                    exc_info=True,
                )
                await asyncio.sleep(delay)
                continue

            self._batches_flushed += 1
            self._rows_inserted += inserted
            self._last_flush_at = time.time()
            await self._maybe_prune()
            return

    async def _maybe_prune(self) -> None:
        policy = self._retention
        if policy is None:
            return

        now = time.time()
        if (
            self._last_prune_at is not None
            and now - self._last_prune_at < policy.prune_interval_seconds
        ):
            return

        try:
            deleted = await self._repository.prune(
                max_rows=policy.max_rows,
                max_age_seconds=policy.max_age_seconds,
                now=now,
            )
        except Exception:
            self._logger.exception("Telemetry retention prune failed")
            return

        self._pruned_rows += deleted
        self._last_prune_at = now


__all__ = ["HalfSyncHalfAsyncPersistenceWorker", "TelemetryRepository", "PersistenceStats"]
