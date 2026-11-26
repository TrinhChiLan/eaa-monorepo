"""Reusable persistence benchmark for the event pipeline.

Drives the real write path -- ``HalfSyncHalfAsyncPersistenceWorker`` into
``TelemetryRepository`` into SQLite/WAL -- and records the numbers that matter
for the constrained edge profile: bytes written, read/write latency percentiles,
queue depth, flush and checkpoint time, RAM, and database/WAL growth.

The production modules are not modified: instrumentation is a duck-typed wrapper
around the repository (the worker only calls ``free_bytes``, ``bulk_insert`` and
``prune``) plus OS counters.

Usage::

    python -m event_pipeline.bench_persistence --help
    make bench
    make bench-smoke

What "bytes written" means here
-------------------------------
``wchar`` from ``/proc/self/io`` is the primary metric: bytes this process handed
to the filesystem, incremented at syscall time.  ``write_bytes`` is recorded too,
but writeback is attributed asynchronously (and on container/overlay filesystems
it often stays at 0 for a whole run), so it is reported only when non-zero.
Neither number measures physical NAND wear; use the device's SMART/TBW data for
that.

Checkpoint timing
-----------------
SQLite's internal autocheckpoint cannot be timed from SQL, so ``manual`` mode
(the default, and the same approach as EDGE_OPTIMIZATION_REPORT.md §2) disables
it and performs timed ``PRAGMA wal_checkpoint`` calls once the WAL passes
``--checkpoint-bytes``.  ``auto`` mode leaves production's 1000-page
autocheckpoint in place; checkpoint latency is then reported as unavailable and
WAL volume is estimated from observed WAL shrinkage.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import platform
import shutil
import sqlite3
import sys
import tempfile
import time
import uuid
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from db_client.database import Database
from db_client.repository import HalfSyncHalfAsyncPersistenceWorker, TelemetryRepository
from mock_data.schemas.events import EventEnvelope, EventSource, SensorSnapshotPayload
from mock_data.schemas.telemetry import PersistenceStats, RetentionPolicy

_PROC_IO = Path("/proc/self/io")
_PROC_STATUS = Path("/proc/self/status")
_PROC_DISKSTATS = Path("/proc/diskstats")

_TOPIC_SENSOR = "sensor.snapshot"
_SECTOR_BYTES = 512


# ---------------------------------------------------------------------------
# OS metric readers.  All file access lives in synchronous helpers so the async
# code never performs blocking I/O itself.
# ---------------------------------------------------------------------------


def _read_proc_io() -> dict[str, int] | None:
    """Return the process I/O counters, or ``None`` when unavailable."""
    try:
        text = _PROC_IO.read_text(encoding="utf-8")
    except OSError:
        return None
    counters: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        try:
            counters[key.strip()] = int(value.strip())
        except ValueError:
            continue
    return counters or None


def _read_proc_status() -> dict[str, int] | None:
    """Return memory counters in kB (``VmRSS``/``VmHWM``), if present."""
    try:
        text = _PROC_STATUS.read_text(encoding="utf-8")
    except OSError:
        return None
    counters: dict[str, int] = {}
    for line in text.splitlines():
        key, _, value = line.partition(":")
        if key not in {"VmRSS", "VmHWM"}:
            continue
        try:
            counters[key] = int(value.split()[0])
        except (IndexError, ValueError):
            continue
    return counters or None


def _read_diskstats(major: int, minor: int) -> dict[str, int] | None:
    """Return sector counters for one block device from ``/proc/diskstats``."""
    try:
        text = _PROC_DISKSTATS.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 10:
            continue
        try:
            if int(parts[0]) != major or int(parts[1]) != minor:
                continue
            return {"sectors_written": int(parts[9])}
        except ValueError:
            continue
    return None


def _device_of(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return os.major(stat.st_dev), os.minor(stat.st_dev)


def _file_size(path: Path) -> int:
    """Size of ``path`` in bytes, or 0 when it does not exist."""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _sizes(db_path: Path) -> dict[str, int]:
    return {
        "db_bytes": _file_size(db_path),
        "wal_bytes": _file_size(Path(f"{db_path}-wal")),
        "shm_bytes": _file_size(Path(f"{db_path}-shm")),
    }


def _sync_filesystem() -> None:
    """Best-effort flush so deferred writeback is visible to the OS counters."""
    try:
        os.sync()
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


def _percentile(ordered: Sequence[float], percentile: float) -> float | None:
    """Linear-interpolated percentile over an already sorted sequence."""
    count = len(ordered)
    if count == 0:
        return None
    if count == 1:
        return float(ordered[0])
    rank = (percentile / 100.0) * (count - 1)
    low = int(rank)
    high = min(low + 1, count - 1)
    weight = rank - low
    return float(ordered[low]) + (float(ordered[high]) - float(ordered[low])) * weight


def summarize(samples: Sequence[float]) -> dict[str, float | int | None]:
    """Return count/min/mean/max plus p50/p95/p99 for one series."""
    if not samples:
        return {
            "n": 0,
            "min": None,
            "mean": None,
            "max": None,
            "p50": None,
            "p95": None,
            "p99": None,
        }
    ordered = sorted(float(value) for value in samples)
    return {
        "n": len(ordered),
        "min": ordered[0],
        "mean": sum(ordered) / len(ordered),
        "max": ordered[-1],
        "p50": _percentile(ordered, 50.0),
        "p95": _percentile(ordered, 95.0),
        "p99": _percentile(ordered, 99.0),
    }


def _summarize_ms(samples: Sequence[float]) -> dict[str, Any]:
    """Summarize a millisecond series, rounding floats for stable output."""
    summary = summarize(samples)
    return {
        key: (round(float(value), 3) if isinstance(value, float) else value)
        for key, value in summary.items()
    }


def _round(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(float(value), digits)


# ---------------------------------------------------------------------------
# Instrumented repository
# ---------------------------------------------------------------------------


class TimedRepository:
    """Records flush/prune latency and submit-to-durable latency.

    Duck-types the three methods the worker uses, delegating to a real
    ``TelemetryRepository`` so production code stays untouched.
    """

    def __init__(self, inner: TelemetryRepository, *, sidecar_limit: int) -> None:
        self._inner = inner
        self._sidecar_limit = max(sidecar_limit, 1)
        self.flush_ms: list[float] = []
        self.prune_ms: list[float] = []
        self.end_to_end_ms: list[float] = []
        self.prune_calls = 0
        self.prune_rows = 0
        self.evicted_unpersisted = 0
        self._submitted_at: OrderedDict[str, float] = OrderedDict()

    def free_bytes(self) -> int | None:
        return self._inner.free_bytes()

    def note_submitted(self, event_id: str) -> None:
        """Record when an event entered the queue so durability can be timed."""
        self._submitted_at[event_id] = time.perf_counter()
        while len(self._submitted_at) > self._sidecar_limit:
            self._submitted_at.popitem(last=False)
            self.evicted_unpersisted += 1

    @property
    def pending(self) -> int:
        return len(self._submitted_at)

    async def bulk_insert(self, events: Sequence[EventEnvelope]) -> int:
        started = time.perf_counter()
        try:
            return await self._inner.bulk_insert(events)
        finally:
            finished = time.perf_counter()
            self.flush_ms.append((finished - started) * 1000.0)
            for event in events:
                submitted_at = self._submitted_at.pop(event.event_id, None)
                if submitted_at is not None:
                    self.end_to_end_ms.append((finished - submitted_at) * 1000.0)

    async def prune(self, **kwargs: Any) -> int:
        started = time.perf_counter()
        try:
            deleted = await self._inner.prune(**kwargs)
        finally:
            self.prune_ms.append((time.perf_counter() - started) * 1000.0)
        self.prune_calls += 1
        self.prune_rows += deleted
        return deleted


# ---------------------------------------------------------------------------
# Sampling state
# ---------------------------------------------------------------------------


@dataclass
class _State:
    started_perf: float = 0.0
    timeseries: list[dict[str, Any]] = field(default_factory=list)
    enqueue_ms: list[float] = field(default_factory=list)
    read_fetch_ms: list[float] = field(default_factory=list)
    read_count_ms: list[float] = field(default_factory=list)
    checkpoint_ms: list[float] = field(default_factory=list)
    checkpoint_bytes: int = 0
    checkpoint_count: int = 0
    checkpoint_errors: int = 0
    wal_appended: int = 0
    last_wal: int = 0
    wal_peak: int = 0
    db_peak: int = 0
    rss_peak_kb: int = 0
    submitted: int = 0


# ---------------------------------------------------------------------------
# Load generation and observers
# ---------------------------------------------------------------------------


def _make_sensor_event(sequence: int) -> EventEnvelope:
    """Build a realistic sensor snapshot through the canonical contract."""
    now = time.time()
    return EventEnvelope.create(
        payload=SensorSnapshotPayload(
            motor_current_amp=1.2,
            infrared_temp_celsius=41.5,
            conveyor_speed_mms=100.0,
            optical_curtain_clear=True,
            estop_circuit_closed=True,
            sensor_timestamp_ms=int(now * 1000),
        ),
        source=EventSource.SENSOR,
        topic=_TOPIC_SENSOR,
        sequence=sequence,
        event_time=now,
        event_id=uuid.uuid4().hex,
    )


async def _produce(
    worker: HalfSyncHalfAsyncPersistenceWorker,
    repository: TimedRepository,
    state: _State,
    args: argparse.Namespace,
    stop: asyncio.Event,
) -> None:
    loop = asyncio.get_running_loop()
    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    next_at = loop.time()
    sequence = 0
    while not stop.is_set():
        event = _make_sensor_event(sequence)
        sequence += 1
        repository.note_submitted(event.event_id)
        started = time.perf_counter()
        await worker.submit(event)
        state.enqueue_ms.append((time.perf_counter() - started) * 1000.0)
        state.submitted += 1

        if interval > 0:
            next_at += interval
            delay = next_at - loop.time()
            if delay > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
        elif sequence % 64 == 0:
            # Saturating mode still has to yield or the worker never runs.
            await asyncio.sleep(0)


def _observe_wal(state: _State, size: int) -> None:
    """Accumulate bytes appended to the WAL during the measured window.

    Only growth counts.  A decrease means the WAL was truncated by a
    checkpoint, so the bytes up to the previous observation are already
    accounted for; the reset itself adds nothing.  ``wal_appended`` therefore
    excludes whatever the WAL held before the run started (schema
    initialisation), which is why it can be smaller than ``wal_peak``.
    """
    if size > state.last_wal:
        state.wal_appended += size - state.last_wal
    state.last_wal = size


def _capture_sample(
    db_path: Path,
    worker: HalfSyncHalfAsyncPersistenceWorker,
    state: _State,
) -> None:
    sizes = _sizes(db_path)
    stats = worker.stats()
    io_counters = _read_proc_io()
    status = _read_proc_status()

    wal = sizes["wal_bytes"]
    _observe_wal(state, wal)
    state.wal_peak = max(state.wal_peak, wal)
    state.db_peak = max(state.db_peak, sizes["db_bytes"])

    rss = status.get("VmRSS") if status else None
    if rss is not None:
        state.rss_peak_kb = max(state.rss_peak_kb, rss)

    last_flush_age = None
    if stats.last_flush_at is not None:
        last_flush_age = _round(max(0.0, time.time() - stats.last_flush_at), 3)

    state.timeseries.append(
        {
            "t_s": _round(time.perf_counter() - state.started_perf, 3),
            "queue_size": stats.queue_size,
            "rss_kb": rss,
            "db_bytes": sizes["db_bytes"],
            "wal_bytes": wal,
            "shm_bytes": sizes["shm_bytes"],
            "rows_inserted": stats.rows_inserted,
            "batches_flushed": stats.batches_flushed,
            "wchar_bytes_total": io_counters.get("wchar") if io_counters else None,
            "write_bytes_total": io_counters.get("write_bytes") if io_counters else None,
            "last_flush_age_s": last_flush_age,
        }
    )


async def _sample_loop(
    db_path: Path,
    worker: HalfSyncHalfAsyncPersistenceWorker,
    state: _State,
    args: argparse.Namespace,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.sample_interval)
            break
        except TimeoutError:
            pass
        _capture_sample(db_path, worker, state)


async def _checkpoint_loop(
    connection: Any,
    db_path: Path,
    state: _State,
    args: argparse.Namespace,
    stop: asyncio.Event,
) -> None:
    pragma = "TRUNCATE" if args.checkpoint_truncate else "PASSIVE"
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.sample_interval)
            break
        except TimeoutError:
            pass
        wal_bytes = _sizes(db_path)["wal_bytes"]
        if wal_bytes < args.checkpoint_bytes:
            continue
        _observe_wal(state, wal_bytes)
        started = time.perf_counter()
        try:
            await connection.execute(f"PRAGMA wal_checkpoint({pragma})")
        except Exception:
            state.checkpoint_errors += 1
            continue
        state.checkpoint_ms.append((time.perf_counter() - started) * 1000.0)
        state.checkpoint_bytes += wal_bytes
        state.checkpoint_count += 1


async def _reader_loop(
    repository: TelemetryRepository,
    state: _State,
    args: argparse.Namespace,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.read_interval)
            break
        except TimeoutError:
            pass
        started = time.perf_counter()
        await repository.fetch_recent(limit=args.read_limit)
        state.read_fetch_ms.append((time.perf_counter() - started) * 1000.0)
        started = time.perf_counter()
        await repository.count()
        state.read_count_ms.append((time.perf_counter() - started) * 1000.0)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _prepare_paths(args: argparse.Namespace) -> tuple[Path, Path | None]:
    if args.db_path:
        return Path(args.db_path).expanduser(), None
    workdir = Path(tempfile.mkdtemp(prefix="eaa-bench-"))
    return workdir / "bench.db", workdir


async def _fetch_one(connection: Any, sql: str) -> Any:
    cursor = await connection.execute(sql)
    try:
        row = await cursor.fetchone()
    finally:
        await cursor.close()
    return row[0] if row else None


async def run_benchmark(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run one benchmark and return ``(summary, timeseries)``."""
    started_at = datetime.now(UTC).isoformat()
    db_path, workdir = _prepare_paths(args)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    database = Database(db_path)
    repository = TelemetryRepository(database)
    state = _State()
    stop = asyncio.Event()
    flags = {"interrupted": False}
    timed = TimedRepository(repository, sidecar_limit=args.queue_capacity * 4)

    retention = (
        RetentionPolicy(
            max_rows=args.retention_max_rows,
            prune_interval_seconds=args.prune_interval,
        )
        if args.retention_max_rows > 0
        else None
    )
    worker = HalfSyncHalfAsyncPersistenceWorker(
        timed,  # type: ignore[arg-type]  # duck-typed wrapper around the repository
        queue_capacity=args.queue_capacity,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        max_retries=args.max_retries,
        persist_event_types=None,
        retention=retention,
        min_free_bytes=args.min_free_bytes,
    )

    try:
        await repository.initialize()
        if args.checkpoint_mode == "manual":
            # Make checkpoints observable: production lets SQLite checkpoint
            # internally at 1000 pages, which cannot be timed.
            await database.connection.execute("PRAGMA wal_autocheckpoint=0")

        await worker.start()

        sizes_start = _sizes(db_path)
        io_start = _read_proc_io()
        status_start = _read_proc_status()
        device = _device_of(db_path)
        disk_start = _read_diskstats(*device) if device is not None else None
        page_size = int(await _fetch_one(database.connection, "PRAGMA page_size") or 0)
        journal_mode = str(
            await _fetch_one(database.connection, "PRAGMA journal_mode") or "unknown"
        )

        _install_stop_handler(stop, flags)
        state.started_perf = time.perf_counter()
        state.last_wal = sizes_start["wal_bytes"]

        tasks = [
            asyncio.create_task(_produce(worker, timed, state, args, stop), name="bench-producer"),
            asyncio.create_task(
                _sample_loop(db_path, worker, state, args, stop), name="bench-sampler"
            ),
        ]
        if args.read_interval > 0:
            tasks.append(
                asyncio.create_task(
                    _reader_loop(repository, state, args, stop), name="bench-reader"
                )
            )
        if args.checkpoint_mode == "manual":
            tasks.append(
                asyncio.create_task(
                    _checkpoint_loop(database.connection, db_path, state, args, stop),
                    name="bench-checkpoint",
                )
            )

        try:
            await asyncio.wait_for(stop.wait(), timeout=args.duration)
        except TimeoutError:
            pass

        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Let the worker drain so the final flush is included in the samples.
        await worker.stop(drain_timeout=args.drain_timeout)

        if args.checkpoint_mode == "manual":
            wal_bytes = _sizes(db_path)["wal_bytes"]
            _observe_wal(state, wal_bytes)
            started = time.perf_counter()
            pragma = "TRUNCATE" if args.checkpoint_truncate else "PASSIVE"
            try:
                await database.connection.execute(f"PRAGMA wal_checkpoint({pragma})")
                state.checkpoint_ms.append((time.perf_counter() - started) * 1000.0)
                state.checkpoint_bytes += wal_bytes
                state.checkpoint_count += 1
            except Exception:
                state.checkpoint_errors += 1

        _sync_filesystem()
        _capture_sample(db_path, worker, state)

        final_stats = worker.stats()
        sizes_end = _sizes(db_path)
        io_end = _read_proc_io()

        summary = _build_summary(
            args=args,
            started_at=started_at,
            interrupted=flags["interrupted"],
            timed=timed,
            final_stats=final_stats,
            state=state,
            io_start=io_start,
            io_end=io_end,
            status_start=status_start,
            sizes_start=sizes_start,
            sizes_end=sizes_end,
            disk_start=disk_start,
            device=device,
            page_size=page_size,
            journal_mode=journal_mode,
        )
        return summary, state.timeseries
    finally:
        try:
            await worker.stop(drain_timeout=args.drain_timeout)
        except Exception:
            pass
        try:
            await repository.close()
        except Exception:
            pass
        if workdir is not None and not args.keep_db:
            shutil.rmtree(workdir, ignore_errors=True)


def _install_stop_handler(stop: asyncio.Event, flags: dict[str, bool]) -> None:
    """Turn SIGINT into a graceful stop so a partial summary is still written."""
    loop = asyncio.get_running_loop()

    def _handle() -> None:
        flags["interrupted"] = True
        stop.set()

    try:
        loop.add_signal_handler(2, _handle)  # signal.SIGINT
    except (NotImplementedError, RuntimeError):
        pass


def _wal_logical_bytes(state: _State) -> int:
    """Total bytes appended to the WAL over the run."""
    return state.wal_appended


def _build_summary(
    *,
    args: argparse.Namespace,
    started_at: str,
    interrupted: bool,
    timed: TimedRepository,
    final_stats: PersistenceStats,
    state: _State,
    io_start: dict[str, int] | None,
    io_end: dict[str, int] | None,
    status_start: dict[str, int] | None,
    sizes_start: dict[str, int],
    sizes_end: dict[str, int],
    disk_start: dict[str, int] | None,
    device: tuple[int, int] | None,
    page_size: int,
    journal_mode: str,
) -> dict[str, Any]:
    wchar_delta = write_bytes_delta = read_bytes_delta = None
    if io_start is not None and io_end is not None:
        wchar_delta = io_end.get("wchar", 0) - io_start.get("wchar", 0)
        write_bytes_delta = io_end.get("write_bytes", 0) - io_start.get("write_bytes", 0)
        read_bytes_delta = io_end.get("read_bytes", 0) - io_start.get("read_bytes", 0)

    disk_bytes = None
    if disk_start is not None and device is not None:
        disk_end = _read_diskstats(*device)
        if disk_end is not None:
            disk_bytes = (
                disk_end["sectors_written"] - disk_start["sectors_written"]
            ) * _SECTOR_BYTES

    rows_inserted = final_stats.rows_inserted
    elapsed = max(1e-9, time.perf_counter() - state.started_perf)
    bytes_per_row = None
    if wchar_delta is not None and rows_inserted > 0:
        bytes_per_row = wchar_delta / rows_inserted

    depths = [row["queue_size"] for row in state.timeseries]
    max_depth = max(depths, default=0)
    rss_start = status_start.get("VmRSS") if status_start else None
    rss_end = state.timeseries[-1]["rss_kb"] if state.timeseries else None

    return {
        "label": args.label,
        "started_at": started_at,
        "duration_s": _round(elapsed, 3),
        "interrupted": interrupted,
        "config": {
            "rate_hz": args.rate,
            "batch_size": args.batch_size,
            "flush_interval_s": args.flush_interval,
            "queue_capacity": args.queue_capacity,
            "retention_max_rows": args.retention_max_rows,
            "min_free_bytes": args.min_free_bytes,
            "checkpoint_mode": args.checkpoint_mode,
            "checkpoint_bytes": args.checkpoint_bytes,
            "checkpoint_truncate": args.checkpoint_truncate,
            "read_interval_s": args.read_interval,
            "read_limit": args.read_limit,
            "sample_interval_s": args.sample_interval,
        },
        "throughput": {
            "submitted": state.submitted,
            "rows_inserted": rows_inserted,
            "events_per_s": _round(state.submitted / elapsed, 1),
            "batches": final_stats.batches_flushed,
        },
        "io": {
            "wchar_bytes": wchar_delta,
            "write_bytes": write_bytes_delta or None,
            "read_bytes": read_bytes_delta,
            "bytes_per_row": _round(bytes_per_row, 1),
            "wal_logical_bytes": _wal_logical_bytes(state),
            "db_growth_bytes": sizes_end["db_bytes"] - sizes_start["db_bytes"],
            "diskstats_bytes": disk_bytes,
            "availability": {
                "proc_io": "available" if io_start is not None else "unavailable",
                "write_bytes": "available"
                if write_bytes_delta
                else "0 observed (writeback attribution; see module docstring)",
                "diskstats": "available"
                if disk_bytes
                else "unavailable (device not in /proc/diskstats)",
                "wal_logical_bytes": "exact" if args.checkpoint_mode == "manual" else "estimated",
            },
        },
        "latency_ms": {
            "enqueue": _summarize_ms(state.enqueue_ms),
            "flush": _summarize_ms(timed.flush_ms),
            "end_to_end": _summarize_ms(timed.end_to_end_ms),
            "read_fetch_recent": _summarize_ms(state.read_fetch_ms),
            "read_count": _summarize_ms(state.read_count_ms),
            "prune": _summarize_ms(timed.prune_ms),
            "checkpoint": _summarize_ms(state.checkpoint_ms)
            if args.checkpoint_mode == "manual"
            else {
                "n": 0,
                "min": None,
                "mean": None,
                "max": None,
                "p50": None,
                "p95": None,
                "p99": None,
            },
        },
        "queue": {
            "capacity": args.queue_capacity,
            "max_depth": max_depth,
            "mean_depth": _round(sum(depths) / len(depths), 2) if depths else None,
            "p95_depth": _round(_percentile(sorted(depths), 95.0), 2) if depths else None,
            "utilization_pct": _round(100.0 * max_depth / max(args.queue_capacity, 1), 2),
            "dropped_queue_full": final_stats.dropped_queue_full,
            "dropped_low_disk": final_stats.dropped_low_disk,
            "failed_batches": final_stats.failed_batches,
            "stopped_rejected": final_stats.stopped_rejected,
            "unflushed_on_shutdown": final_stats.unflushed_on_shutdown,
            "unpersisted_at_end": timed.pending,
            "evicted_unpersisted": timed.evicted_unpersisted,
            "prune_calls": timed.prune_calls,
            "pruned_rows": timed.prune_rows,
            "checkpoint_count": state.checkpoint_count,
            "checkpoint_errors": state.checkpoint_errors,
        },
        "memory": {
            "rss_start_kb": rss_start,
            "rss_end_kb": rss_end,
            "rss_peak_kb": state.rss_peak_kb or None,
            "hwm_kb": (status_start or {}).get("VmHWM"),
            "rss_delta_kb": (rss_end - rss_start)
            if rss_start is not None and rss_end is not None
            else None,
        },
        "storage": {
            "db_start": sizes_start["db_bytes"],
            "db_end": sizes_end["db_bytes"],
            "db_peak": state.db_peak,
            "wal_start": sizes_start["wal_bytes"],
            "wal_end": sizes_end["wal_bytes"],
            "wal_peak": state.wal_peak,
            "page_size": page_size,
            "journal_mode": journal_mode,
        },
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "cpu_count": os.cpu_count(),
        },
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


_TIMESERIES_FIELDS = (
    "t_s",
    "queue_size",
    "rss_kb",
    "db_bytes",
    "wal_bytes",
    "shm_bytes",
    "rows_inserted",
    "batches_flushed",
    "wchar_bytes_total",
    "write_bytes_total",
    "last_flush_age_s",
)


def write_artifacts(
    summary: dict[str, Any], timeseries: list[dict[str, Any]], out_dir: Path
) -> tuple[Path, Path]:
    """Write ``summary.json`` and ``timeseries.csv`` and return their paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    series_path = out_dir / "timeseries.csv"
    with series_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(_TIMESERIES_FIELDS), extrasaction="ignore")
        writer.writeheader()
        for row in timeseries:
            writer.writerow(row)
    return summary_path, series_path


def _fmt(value: Any, unit: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:,.2f}{unit}"
    if isinstance(value, int):
        return f"{value:,}{unit}"
    return f"{value}{unit}"


def print_table(summary: dict[str, Any], paths: tuple[Path, Path] | None) -> None:
    """Print a compact human-readable report."""
    throughput = summary["throughput"]
    io_stats = summary["io"]
    latency = summary["latency_ms"]
    queue = summary["queue"]
    memory = summary["memory"]
    storage = summary["storage"]

    lines = [
        f"benchmark: {summary['label']}  ({summary['duration_s']}s, "
        f"{summary['config']['checkpoint_mode']} checkpoint)",
        "",
        "throughput",
        f"  submitted            {_fmt(throughput['submitted'])}",
        f"  rows inserted        {_fmt(throughput['rows_inserted'])}",
        f"  events/s             {_fmt(throughput['events_per_s'])}",
        "",
        "bytes",
        f"  wchar (fs writes)    {_fmt(io_stats['wchar_bytes'], ' B')}",
        f"  per row              {_fmt(io_stats['bytes_per_row'], ' B')}",
        f"  WAL logical          {_fmt(io_stats['wal_logical_bytes'], ' B')} "
        f"({io_stats['availability']['wal_logical_bytes']})",
        f"  DB growth            {_fmt(io_stats['db_growth_bytes'], ' B')}",
        "",
        "latency (ms)                  p50        p95        p99        n",
    ]
    for name, series in latency.items():
        lines.append(
            f"  {name:<20} {_fmt(series['p50']):>9} {_fmt(series['p95']):>10} "
            f"{_fmt(series['p99']):>10} {_fmt(series['n']):>8}"
        )
    lines += [
        "",
        "queue",
        f"  max depth            {_fmt(queue['max_depth'])} / {_fmt(queue['capacity'])} "
        f"({_fmt(queue['utilization_pct'], '%')})",
        f"  mean / p95 depth     {_fmt(queue['mean_depth'])} / {_fmt(queue['p95_depth'])}",
        f"  dropped (full/disk)  {_fmt(queue['dropped_queue_full'])} / "
        f"{_fmt(queue['dropped_low_disk'])}",
        "",
        "memory (kB)",
        f"  RSS start / end      {_fmt(memory['rss_start_kb'])} / {_fmt(memory['rss_end_kb'])}",
        f"  RSS peak             {_fmt(memory['rss_peak_kb'])}",
        "",
        "storage",
        f"  DB start / end       {_fmt(storage['db_start'])} / {_fmt(storage['db_end'])} B",
        f"  WAL peak / end       {_fmt(storage['wal_peak'])} / {_fmt(storage['wal_end'])} B",
    ]
    if paths is not None:
        lines += ["", f"artifacts: {paths[0]}  {paths[1]}"]
    print("\n".join(lines))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eaa-bench-persistence",
        description="Benchmark the telemetry persistence path (worker -> SQLite/WAL).",
        epilog=(
            "wchar is bytes written through the filesystem, not physical NAND writes; "
            "use device SMART/TBW data for flash wear."
        ),
    )
    parser.add_argument("--label", default="run", help="Run name used in the output")
    parser.add_argument("--duration", type=float, default=30.0, help="Seconds to run")
    parser.add_argument("--rate", type=float, default=5.0, help="Events/second (0 = saturate)")
    parser.add_argument("--batch-size", type=int, default=50, help="Worker batch size")
    parser.add_argument("--flush-interval", type=float, default=10.0, help="Worker flush interval")
    parser.add_argument("--queue-capacity", type=int, default=10_000, help="Worker queue capacity")
    parser.add_argument("--max-retries", type=int, default=3, help="Worker write retries")
    parser.add_argument("--retention-max-rows", type=int, default=0, help="Enable prune at N rows")
    parser.add_argument("--prune-interval", type=float, default=1.0, help="Prune interval seconds")
    parser.add_argument("--min-free-bytes", type=int, default=0, help="Low-disk drop floor")
    parser.add_argument(
        "--checkpoint-mode",
        choices=("manual", "auto"),
        default="manual",
        help="manual: time explicit checkpoints; auto: production autocheckpoint",
    )
    parser.add_argument(
        "--checkpoint-bytes", type=int, default=4_194_304, help="WAL bytes before checkpoint"
    )
    parser.add_argument(
        "--checkpoint-truncate", action="store_true", help="Use TRUNCATE not PASSIVE"
    )
    parser.add_argument("--read-interval", type=float, default=0.5, help="Seconds between reads")
    parser.add_argument("--read-limit", type=int, default=100, help="fetch_recent limit")
    parser.add_argument("--sample-interval", type=float, default=0.5, help="Sampling interval")
    parser.add_argument("--drain-timeout", type=float, default=10.0, help="Worker drain timeout")
    parser.add_argument("--db-path", default=None, help="Database path (default: temp dir)")
    parser.add_argument("--keep-db", action="store_true", help="Keep the temporary database")
    parser.add_argument(
        "--out", default=None, help="Output dir (default: bench-results/<label>-<ts>)"
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress the human-readable table")
    return parser


def _validate(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.rate < 0:
        parser.error("--rate cannot be negative")
    for name in ("batch_size", "queue_capacity"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    for name in ("flush_interval", "sample_interval", "drain_timeout", "prune_interval"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.read_interval < 0:
        parser.error("--read-interval cannot be negative")
    if args.checkpoint_bytes <= 0:
        parser.error("--checkpoint-bytes must be positive")


def cli(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate(parser, args)

    out_dir = (
        Path(args.out)
        if args.out
        else (
            Path("bench-results") / f"{args.label}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        )
    )

    summary, timeseries = asyncio.run(run_benchmark(args))
    paths = write_artifacts(summary, timeseries, out_dir)
    if not args.quiet:
        print_table(summary, paths)
    return 0


if __name__ == "__main__":
    sys.exit(cli())
